import re
import spacy
from spacy.lang.en.stop_words import STOP_WORDS
import json
from pdfminer.high_level import extract_text
from google.cloud import storage, pubsub_v1
import functions_framework
import os
from io import BytesIO

# Fungsi untuk ekstraksi teks dari PDF
def extract_text_from_pdf(pdf_path):
    try:
        pdf_stream = BytesIO(pdf_path)
        text = extract_text(pdf_stream)
        return text
    except Exception as e:
        return str(e)

# Fungsi untuk pembersihan teks
def clean_text(text):
    nlp = spacy.load("en_core_web_sm")

    # Hapus karakter yang tidak relevan
    text = re.sub(r"[^a-zA-Z0-9\s,:]", "", text)

    doc = nlp(text)

    cleaned_text = []
    for token in doc:
        if token.text.lower() not in STOP_WORDS:
            cleaned_text.append(token.text.lower())

    return " ".join(cleaned_text)

# Ekstraksi Informasi Pribadi
def extract_personal_info(raw_text):
    phone = re.search(r'(?:\(\+62\)|\+62|62|0)\s?8\d{1,2}(?:[-.\s]?\d{2,4}){2,3}', raw_text)
    email = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', raw_text)
    linkedin = re.search(r'(?:https?://)?(?:www\.)?linkedin\.com/\S+', raw_text)
    github = re.search(r'(?:https?://)?(?:www\.)?github\.com/\S+', raw_text)
    degree = re.search(r'Bachelor\s+(?:degree|of)\s+[A-Za-z\s]+', raw_text, re.IGNORECASE)

    # Ambil nama dari baris pertama
    lines = [line.strip() for line in raw_text.split("\n") if line.strip()]
    name = lines[0] if lines else None

    return {
        "Phone Number": phone.group(0) if phone else None,
        "Email": email.group(0) if email else None,
        "Github": github.group(0) if github else None,
        "LinkedIn": linkedin.group(0) if linkedin else None,
        "Degree": degree.group(0) if degree else None
    }

# Ekstraksi Keterampilan
def extract_skills(cleaned_text):
    tech_skills = {
        "Programming Languages": [
            "python", "java", "javascript", "typescript", "c\\+\\+", "c#",
            "ruby", "php", "swift", "kotlin", "go", "rust", "scala"
        ],
        "Web Developer": [
            "html", "css", "react", "angular", "vue", "nodejs", "django",
            "flask", "spring", "laravel", "express", "graphql", "rest api"
        ],
        "Mobile Development": [
            "android", "ios", "flutter", "react native", "kotlin", "swift",
            "xamarin", "ionic", "jetpack compose", "android studio", "dart"
        ],
        "Cloud & DevOps": [
            "aws", "azure", "google cloud", "docker", "kubernetes",
            "jenkins", "terraform", "ansible", "cloud computing",
            "saas", "iaas", "paas"
        ],
        "Data & AI": [
            "machine learning", "data science", "deep learning", "tensorflow",
            "pytorch", "pandas", "numpy", "scikit-learn", "data analysis",
            "big data", "spark", "hadoop", "sql", "nosql", "tableau",
            "artificial intelligence", "computer vision",
            "natural language processing","nlp","ocr",
            "seaborn", "data engineering",
            "data visualization", "r",
            "r studio"
        ],
        "Design & UX": [
            "ui", "ux", "figma", "sketch", "adobe xd", "photoshop",
            "user interface", "user experience", "graphic design"
        ],
        "Databases": [
            "mysql", "postgresql", "mongodb", "sqlite", "redis",
            "oracle", "cassandra", "firebase"
        ],
        "Other Technologies": [
            "git", "linux", "blockchain", "cybersecurity", "network security",
            "agile", "scrum", "microservices", "serverless"
        ]
    }

    all_skills = [skill for category in tech_skills.values() for skill in category]
    extracted_skills = []

    for skill in all_skills:
        if re.search(r'\b' + re.escape(skill.lower()) + r'\b', cleaned_text.lower()):
            extracted_skills.append(skill)

    extracted_skills = sorted(set(extracted_skills))
    return extracted_skills

# Ekstraksi Pengalaman Kerja
def extract_work_experience(raw_text):
    work_pattern = re.compile(r"Work Experiences\s*\n\n(?P<company_name>.+?)\n\n(?P<start_date>\w+\s+\d{4})\s*-\s*(?P<end_date>\w+\s+\d{4}|Present)\n\n(?P<position>.+?)\n\n(?P<description>(?:.+?\n)+?)\n", re.DOTALL)

    matches = work_pattern.finditer(raw_text)
    work_experiences = []

    for match in matches:
        company_name = match.group("company_name").strip()
        start_date = match.group("start_date").strip()
        end_date = match.group("end_date").strip()
        position = match.group("position").strip()
        description = [line.strip() for line in match.group("description").strip().split("\n") if line.strip()]

        work_experiences.append({
            "Company Name": company_name,
            "Start Date": start_date,
            "End Date": end_date,
            "Position": position,
            "Description": description
        })

    return work_experiences

# Ekstraksi Sertifikasi
def extract_certifications(cleaned_text):
    cert_patterns = [r'Certifications?:?\s*(.*?)(?=\n\n|\n[A-Z]|$)', r'Certificates?:?\s*(.*?)(?=\n\n|\n[A-Z]|$)']
    certifications = []

    for pattern in cert_patterns:
        matches = re.findall(pattern, cleaned_text, re.IGNORECASE | re.DOTALL)
        for match in matches:
            match = match.replace("\n", " ")
            certifications.extend([re.sub(r'\s+', ' ', cert.strip()) for cert in match.split(',') if cert.strip()])

    return list(set(certifications))

# Fungsi untuk memparse CV ke dalam format JSON
def parse_cv_to_json(raw_text, cleaned_text):
    lines = [line.strip() for line in cleaned_text.split("\n") if line.strip()]
    name = lines[0] if lines else "Unknown"

    return {
        name: {
            "Personal Info": extract_personal_info(raw_text),
            "Skills": extract_skills(cleaned_text),
            "Work Experience": extract_work_experience(raw_text),
            "Certification": extract_certifications(cleaned_text)
        }
    }

# Fungsi untuk menyimpan hasil parsing ke dalam file JSON
def save_json(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

@functions_framework.cloud_event
def main(cloud_event):
    data = cloud_event.data

    # cloud storage info
    bucket_name = data['bucket']
    filename = data['name']

    # env vars
    project_id = os.environ.get("PROJECT_ID")
    response_topic = os.environ.get("FUNCTION_CV_PARSING_RESPONSE_TOPIC")
    start = os.environ.get("FUNCTION_CV_DIR", None);

    # exceptions
    if start == None:
        print("[ERR]: missing env vars")
        return
 
    if not filename.endswith('.pdf'):
        print(f"[ERR]: invalid filename: {filename}")
        return
    if not filename.startswith(start):
        print(f"[INFO]: ignore file: {filename}")
        return

    # bucket init
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob(filename)
    pdf_content = blob.download_as_bytes()
    print(f"[DEBUG]: parsing result: {pdf_content}")

    # parsing
    extracted_text = extract_text_from_pdf(pdf_content)
    cleaned_text = clean_text(extracted_text)
    result = parse_cv_to_json(extracted_text, cleaned_text)
    print(f"[DEBUG]: parsing result: {extracted_text}")
    print(f"[DEBUG]: parsing result: {cleaned_text}")
    
    # upload result
    new_filename = filename.replace('.pdf', '.json')
    upload_blob = bucket.blob(new_filename)
    upload_blob.upload_from_string(
        data=json.dumps(result, indent=4, ensure_ascii=False),
        content_type='application/json',
    )
    
    # signaling
    publisher = pubsub_v1.PublisherClient()
    topic = publisher.topic_path(project_id, response_topic)
    response = {
        "error": False,
        "filename": new_filename
    }
    future = publisher.publish(topic, data=json.dumps(response).encode("utf-8"))
    print(f"[INFO]: Pub/Sub message published: {future.result()}")

    return