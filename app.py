from fastapi import FastAPI, HTTPException 
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from mistralai import Mistral
import os
import logging
import pymysql
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
import psycopg2
from psycopg2.extras import execute_values
import uvicorn

load_dotenv()
MISTRAL_KEY = os.getenv("MISTRAL_API_KEY")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

mistral_client = Mistral(api_key=MISTRAL_KEY)
model = SentenceTransformer("intfloat/multilingual-e5-base")

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins. Change this to specific domains in production.
    allow_credentials=True,
    allow_methods=["*"],  # Allows all HTTP methods (GET, POST, OPTIONS, etc.)
    allow_headers=["*"],  # Allows all headers
)

class InputData(BaseModel):
    user_input: str
    documents: list[str] 

def connect_database():
    try:
        return psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
    except psycopg2.Error as e:
        logging.error(f"Error al conectar con la base de datos: {e}")
        raise e
def embbed_question(text):
    return model.encode(text)


@app.post("/submit/")
async def submit_data(data: InputData):
    question = data.user_input
    available_documents = data.documents
    query_embedding = embbed_question(question)
    embedding_str = "[" + ",".join(map(str, query_embedding)) + "]"
    #Moodle docs 
    search_query = """
        SELECT contenthash, text, 1 - (embedding <=> %s) AS similarity
        FROM moodle_docs
        WHERE contenthash IN %s
        ORDER BY similarity DESC
        LIMIT 4
    """
    try:
        with connect_database() as conn, conn.cursor() as cursor:
            print("Received documents: ", available_documents)
               
            cursor.execute(search_query, (embedding_str, tuple(available_documents)))
            results = cursor.fetchall()
         
            if not results:
                return {"message": "No se encontraron resultados relevantes en la base de datos."}
            
            retrieved_texts = [result[1] for result in results]
            documents = [result[0] for result in results]
            
            combined_text = "\n".join(retrieved_texts)
            documents_array = "\n".join(documents)
            
            prompt = f"""
            Eres un asistente experto en responder preguntas basadas en información relevante.  
            Usa el siguiente contexto para responder la pregunta del usuario de manera clara y concisa.  
            Si el contexto no contiene información suficiente, indícalo en la respuesta.

            ### CONTEXTO:
            {combined_text}

            ### PREGUNTA DEL USUARIO:
            {question}

            ### FRAGMENTOS DE TEXTO DE DOCUMENTOS:
            Los siguientes fragmentos de texto provienen de diferentes documentos y están separados por saltos de línea. Cada fragmento puede contener información relevante para responder a la pregunta del usuario. {documents_array}

            ### INSTRUCCIONES:
            - Usa únicamente el contexto proporcionado para responder.  
            - Sé directo y claro en la respuesta.  
            - Contesta en el idioma de la pregunta del usuario.
            - Si la información no es suficiente, di: "No encontré suficiente información en el contexto proporcionado".  
            - Añade en la respuesta el nombre del documento de donde se extrae la información. Si el nombre del documento no se encuentra explicito en el fragmento de texto, omite esa parte de la respuesta.
            """
            chat_response = mistral_client.chat.complete(
                model="open-mistral-nemo",
                messages=[{"role": "user", "content": prompt}]
            )
            response_content = chat_response.choices[0].message.content
            print("response_content: ",response_content)

            return {"response": response_content}
    except Exception as e:
        print("Error occurred: ", str(e))
        return {"error": f"Error: {str(e)}"}

