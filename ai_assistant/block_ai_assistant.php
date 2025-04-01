<?php
defined('MOODLE_INTERNAL') || die();

class block_ai_assistant extends block_base {
    public function init() {
        $this->title = get_string('pluginname', 'block_ai_assistant');
    }

    public function get_content() {
        global $COURSE, $DB, $OUTPUT;
    
        if ($this->content !== null) {
            return $this->content;
        }
    
        $this->content = new stdClass();
        $this->content->text = '';

        $sql = "SELECT path FROM {context} WHERE contextlevel = 50 AND instanceid = :courseid";
        $course_path = $DB->get_field_sql($sql, ['courseid' => $COURSE->id]);
    
        if (!$course_path) {
            $this->content->text .= get_string('nofiles', 'block_ai_assistant');
            return $this->content;
        }
    
        $like_pattern = $course_path . "/%"; 
    
        $sql = "SELECT f.contenthash
                FROM {files} f
                JOIN {context} ctx ON f.contextid = ctx.id
                JOIN {course_modules} cm ON cm.id = ctx.instanceid
                JOIN {modules} m ON m.id = cm.module
                WHERE ctx.contextlevel = 70
                AND f.filename <> '.' 
                AND cm.visible = 1
                AND ctx.path LIKE :likepattern";
    
        $files = $DB->get_records_sql($sql, ['likepattern' => $like_pattern]);
        
        $content_hash = [];
        if ($files) {
            foreach ($files as $file) {
                $content_hash[] = $file->contenthash;
            }
        } else {
            $this->content->text .= get_string('nofiles', 'block_ai_assistant');
        }
        $this->content->text .= '<p><i>' . get_string('assistant_title', 'block_ai_assistant') .'</i></p>';
        $this->content->text .= '<form id="myForm">';
        $this->content->text .= '<label for="user_input">'. get_string('label_question', 'block_ai_assistant') .'</label>';
        $this->content->text .= '<input type="text" id="user_input" name="user_input" autocomplete="off" />';
        $this->content->text .= '<input type="hidden" name="documents" value="' . htmlspecialchars(json_encode($content_hash)) . '" />';
        $this->content->text .= '<input type="submit" value="' . get_string('submit_button', 'block_ai_assistant') . '" />';
        $this->content->text .= '</form>';

        $this->content->text .= '<div id="result"></div>';

        $this->content->text .= "
        <script>
            document.getElementById('myForm').addEventListener('submit', function(event) {
                event.preventDefault();

                var userInput = document.getElementById('user_input').value.trim();
                if (!userInput) {  
                    alert('" . get_string('error_message', 'block_ai_assistant') . "'); 
                    return;
                }
                var documents = document.getElementsByName('documents')[0].value;
                var documentsParsed = JSON.parse(documents);
                fetch('http://127.0.0.1:8000/submit', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        user_input: userInput,
                        documents: documentsParsed
                    })
                })
                .then(response => response.json())
                .then(data => {
                    document.getElementById('result').innerText = '" . get_string('api_response', 'block_ai_assistant') . "' + data.response;
                })
                .catch(error => {
                    document.getElementById('result').innerText = '" . get_string('api_error', 'block_ai_assistant') . "' + error.message;
                });
            });
        </script>
        ";

        return $this->content;
    }
}
