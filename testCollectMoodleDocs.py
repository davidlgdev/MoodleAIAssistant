import os
import logging
import shutil
import mysql.connector
import psycopg2
import sys
import pdfplumber
from psycopg2.extras import execute_values
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DB_CONFIG = {
    "host": os.getenv("DB_MOODLE_HOST"),
    "user": os.getenv("DB_MOODLE_USER"),
    "password": os.getenv("DB_MOODLE_PASSWORD"),
    "database": os.getenv("DB_MOODLE_NAME"),
}

MOODLE_DATA_PATH = os.getenv("MOODLE_DATA_PATH", "C:/xampp/moodledata/filedir/")
TEMP_PDF_FOLDER = "Temporary_PDFs"
os.makedirs(TEMP_PDF_FOLDER, exist_ok=True)

model = SentenceTransformer("intfloat/multilingual-e5-base")

def connect_database_pgvector():
    try:
        return psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
    except psycopg2.Error as e:
        logging.error(f"Error al conectar con la base de datos: {e}")
        sys.exit(1)

def update_pgvector_with_moodle():
    try:
        conexion = mysql.connector.connect(**DB_CONFIG)
        cursor = conexion.cursor(dictionary=True)

        consulta = """
        SELECT DISTINCT contenthash, filename 
        FROM mdl_files 
        WHERE filename <> '' 
        AND mimetype = 'application/pdf' 
        AND component NOT IN ('tool_recyclebin', 'user', 'assignfeedback_editpdf');
        """
        cursor.execute(consulta)
        archivos = cursor.fetchall()
    
        sync_pgvector_moodle(archivos) 
        archivos_a_procesar = verify_pgvector_docs(archivos)
        if archivos_a_procesar:
            procesar_pdfs(archivos_a_procesar)
            
    except mysql.connector.Error as err:
        logging.error(f"Error en la base de datos: {err}")

    finally:
        if 'conexion' in locals() and conexion.is_connected():
            cursor.close()
            conexion.close()

def sync_pgvector_moodle(archivos_moodle):
    #Compara los contenthash de los archivos de Moodle y los contenthash de los archivos de PgVector, y los que ya no están en Moodle se eliminan de PgVector
    contenthash_moodle = {archivo["contenthash"] for archivo in archivos_moodle}

    try:
        with connect_database_pgvector() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT contenthash FROM moodle_docs;")
            contenthash_pgvector = {row[0] for row in cursor.fetchall()}
            registros_a_eliminar = contenthash_pgvector - contenthash_moodle

            if registros_a_eliminar:
                query_delete = "DELETE FROM moodle_docs WHERE contenthash IN %s;"
                cursor.execute(query_delete, (tuple(registros_a_eliminar),))
                conn.commit()
                logging.info(f"🗑️ Eliminados {len(registros_a_eliminar)} registros obsoletos de pgvector.")

    except psycopg2.Error as e:
        logging.error(f"Error al sincronizar pgvector con Moodle: {e}")

def verify_pgvector_docs(archivos_moodle):
    #Compara los contenthash de los archivos de Moodle y los contenthash de los archivos de PgVector, y los que no estan en PgVector se añaden
    contenthash_moodle = {archivo["contenthash"] for archivo in archivos_moodle}

    try:
        with connect_database_pgvector() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT contenthash FROM moodle_docs;")
            contenthash_pgvector = {row[0] for row in cursor.fetchall()}
            archivos_a_procesar = [archivo for archivo in archivos_moodle if archivo["contenthash"] not in contenthash_pgvector]
            logging.info(f"📄 {len(archivos_a_procesar)} archivos a procesar.")
            return archivos_a_procesar

    except psycopg2.Error as e:
        logging.error(f"Error al verificar documentos en pgvector: {e}")
        return []

def embbed_document(text):
    if not text:
        logging.warning("No se encontraron fragmentos en el documento.")
        return [], []

    embeddings = model.encode(text)
    return text, embeddings

def create_table():
    query = """
        CREATE TABLE IF NOT EXISTS moodle_docs (
            id SERIAL PRIMARY KEY,
            contenthash TEXT,
            document_name TEXT,
            text TEXT,  -- El fragmento de texto
            embedding vector(768),
            UNIQUE(contenthash, text) 
        );
    """
    try:
        with connect_database_pgvector() as conn, conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
            logging.info("Tabla 'moodle_docs' verificada correctamente.")
    except psycopg2.Error as e:
        logging.error(f"Error al crear la tabla: {e}")

def load_into_DB(contenthash, embeddings_list, chunks_list, document_name):
    if len(embeddings_list) != len(chunks_list):
        logging.error("La lista de embeddings y la lista de chunks tienen diferentes longitudes.")
        return
    
    create_table()

    insert_query = """
        INSERT INTO moodle_docs (contenthash, document_name, text, embedding)
        VALUES %s
        ON CONFLICT (contenthash, text) DO UPDATE 
        SET document_name = EXCLUDED.document_name, 
            embedding = EXCLUDED.embedding;
    """
    data = [(contenthash, document_name, chunks_list[i], embeddings_list[i].tolist()) for i in range(len(chunks_list))]

    try:
        with connect_database_pgvector() as conn, conn.cursor() as cursor:
            execute_values(cursor, insert_query, data)
            conn.commit()
            logging.info(f"🔄 {len(data)} fragmentos insertados/actualizados en pgvector.")
    except psycopg2.Error as e:
        logging.error(f"Error al cargar en la base de datos: {e}")

def procesar_pdfs(archivos):
    for archivo in archivos:
        contenthash = archivo['contenthash']
        filename = archivo['filename']

        source_path = os.path.join(MOODLE_DATA_PATH, contenthash[:2], contenthash[2:4], contenthash)
        temp_pdf_path = os.path.join(TEMP_PDF_FOLDER, filename)

        if os.path.exists(source_path):
            shutil.copy(source_path, temp_pdf_path)
            logging.info(f"✅ Archivo copiado: {temp_pdf_path}")

            sections = load_and_divide_document(temp_pdf_path, filename)
            if sections:
                chunks_list, embeddings_list = embbed_document(sections)
                load_into_DB(contenthash, embeddings_list, chunks_list, filename)

            os.remove(temp_pdf_path)
            logging.info(f"🗑️ Eliminado: {temp_pdf_path}")
        else:
            logging.warning(f"Archivo no encontrado en moodledata: {source_path}")


def load_and_divide_document(file_path, doc_name):
    try:
        sections = []
        current_section = None
        max_length = 2000
        overlap = 200
        title_font_threshold = 12 #Cambiar dependiendo del doc (si no es una guia)
        min_characters = 3
        temp_title = ""
        untitled_content = ""  

        with pdfplumber.open(file_path) as pdf:
            for i, page in enumerate(pdf.pages[2:]):  # Empezar desde la tercera página
                page_objs = page.extract_words(extra_attrs=["size"])
                
                if not page_objs:
                    continue 
                
                for obj in page_objs:
                    text_fragment = obj.get("text", "").strip()
                    font_size = obj.get("size", 0)
                    print(f"Font size: {font_size}")  # Verifica los tamaños de las fuentes
                    
                    if len(text_fragment) < min_characters:
                        continue
                        
                    if font_size > title_font_threshold:  
                        if untitled_content and not current_section:
                            current_section = {"title": "Sin título", "content": untitled_content}
                            sections.append(current_section)
                            untitled_content = ""
                            current_section = None
                            
                        temp_title += " " + text_fragment if temp_title else text_fragment
                    else:
                        if temp_title:
                            if current_section:
                                sections.append(current_section)
                            current_section = {"title": temp_title.strip(), "content": ""}
                            temp_title = ""  
                        
                        if current_section:
                            current_section["content"] += text_fragment + " "
                        else:
                            untitled_content += text_fragment + " "
                
                if current_section:
                    current_section["content"] += "\n"
                elif untitled_content:
                    untitled_content += "\n"

        if current_section:
            sections.append(current_section)
        elif untitled_content:
            sections.append({"title": "", "content": untitled_content})

        formatted_sections = []
        for sec in sections:
            content = sec["content"].strip()
            
            if len(content) < min_characters:
                continue
                
            if len(content) <= max_length:
                formatted_sections.append(f'{doc_name} - {sec["title"]}: {content}')
            else:
                start = 0
                while start < len(content):
                    end = min(start + max_length, len(content))
                    fragment = content[start:end]
                    if start > 0:
                        fragment = content[start - overlap:end]
                    
                    if len(fragment.strip()) >= min_characters:
                        formatted_sections.append(f'{doc_name} - {sec["title"]}: {fragment.strip()}')
                    start += max_length

        return formatted_sections if formatted_sections else None

    except Exception as e:
        logging.error(f"Error procesando {file_path}: {e}")
        return None

if __name__ == "__main__":
    update_pgvector_with_moodle()
