import os
import yaml
import logging
import google.cloud.logging
from flask import Flask, render_template, request

import firebase_admin
from firebase_admin import firestore

from google.cloud import aiplatform
import vertexai
from vertexai.generative_models import GenerativeModel
from vertexai.language_models import TextEmbeddingModel

# Configure Cloud Logging
logging_client = google.cloud.logging.Client()
logging_client.setup_logging()
logging.basicConfig(level=logging.INFO)

# Project-specific parameters
PROJECT_ID = "qwiklabs-gcp-02-d38fbed040bf"
REGION = "us-central1"
INDEX_ENDPOINT="projects/275307539691/locations/us-central1/indexEndpoints/3605989120779747328"
DEPLOYED_INDEX_ID="assessment_index_deployed"

# Initialize Vertex AI
aiplatform.init(project=PROJECT_ID, location=REGION)

# Instantiating the Firebase client
firebase_app = firebase_admin.initialize_app()
db = firestore.client()

# Instantiate an embedding model here
embedding_model = TextEmbeddingModel.from_pretrained("text-embedding-004")

# Instantiate a Generative AI model here
gen_model = GenerativeModel("gemini-1.5-flash-001", generation_config={"temperature":0})

# Helper function that reads from the config file.
def get_config_value(config, section, key, default=None):
    """
    Retrieve a configuration value from a section with an optional default value.
    """
    try:
        return config[section][key]
    except:
        return default

# Open the config file (config.yaml)
with open("config.yaml") as f:
    config = yaml.safe_load(f)

# Read application variables from the config file
TITLE = get_config_value(config, "app", "title", "Ask Google")
SUBTITLE = get_config_value(config, "app", "subtitle", "Your friendly Bot")
CONTEXT = get_config_value(
    config, "gemini", "context", "You are a bot who can answer all sorts of questions"
)
BOTNAME = get_config_value(config, "gemini", "botname", "Google")

app = Flask(__name__)

# The Home page route
@app.route("/", methods=["POST", "GET"])
def main():
    if request.method == "GET":
        question = ""
        answer = "Hi, I'm FreshBot. What are your food safety questions?"
    else:
        question = request.form["input"]
        logging.info(
            question,
            extra={"labels": {"service": "cymbal-service", "component": "question"}},
        )
        data = search_vector_database(question)
        answer = ask_gemini(question, data)

    logging.info(
        answer, extra={"labels": {"service": "cymbal-service", "component": "answer"}}
    )
    print("Answer: " + answer)

    model = {
        "title": TITLE,
        "subtitle": SUBTITLE,
        "botname": BOTNAME,
        "message": answer,
        "input": question,
    }

    return render_template("index.html", model=model)

def search_vector_database(question):
    data = ""
    
    question_embedding = embedding_model.get_embeddings([question])[0].values

    matching_engine_index_endpoint = aiplatform.MatchingEngineIndexEndpoint(
        index_endpoint_name=INDEX_ENDPOINT
    )

    try:
        matched_neighbors = matching_engine_index_endpoint.find_neighbors(
            deployed_index_id=DEPLOYED_INDEX_ID,
            queries=[question_embedding],
            num_neighbors=5
        )
    except Exception as e:
        logging.error(f"Error in vector database search: {str(e)}")
        return "Error: Unable to search the database."

    matched_ids = [neighbor.id for neighbor in matched_neighbors[0]]

    pdf_pages_ref = db.collection("pdf_pages")
    docs = [pdf_pages_ref.document(doc_id).get() for doc_id in matched_ids]

    data = "\n".join([doc.to_dict()['page'] for doc in docs if doc.exists])

    logging.info(
        data, extra={"labels": {"service": "cymbal-web-ui-service", "component": "data"}}
    )
    return data

def ask_gemini(question, data):
    system_prompt = """You are an AI assistant with a wide range of knowledge. Your primary task is to answer questions based on the provided data, but you can also draw on your general knowledge when appropriate. Follow these guidelines:

    1. If the question can be fully answered using the provided data, do so.
    2. If the question is partially related to the data, use the data as context and supplement with your general knowledge.
    3. If the question is not related to the data at all, answer based on your general knowledge.
    4. If you're unsure or don't have enough information to answer accurately, say so.
    5. Always strive to provide helpful, accurate, and ethically sound information.
    6. If asked about opinions or subjective matters, clarify that these are complex topics with various viewpoints.

    Remember to be respectful, avoid harmful content, and maintain user privacy."""

    prompt = f"{system_prompt}\n\nProvided Data:\n{data}\n\nUser's Question: {question}\n\nAnswer:"

    response = gen_model.generate_content(prompt)
    return response.text

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
