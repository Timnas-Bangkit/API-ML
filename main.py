import re
import spacy
from spacy.lang.en.stop_words import STOP_WORDS
import json
from pdfminer.high_level import extract_text
from google.cloud import storage, pubsub_v1
import functions_framework
import os
from io import BytesIO
from transformers import BertTokenizer, TFBertModel
from datetime import datetime
import numpy as np

job_roles = {
    # Data Engineer
    'data': 'data engineer',
    'pipelines': 'data engineer',
    'etl': 'data engineer',
    'sql': 'data engineer',
    'bigquery': 'data engineer',
    'spark': 'data engineer',
    'kafka': 'data engineer',
    'hadoop': 'data engineer',
    'data storage': 'data engineer',
    'python': 'data engineer',
    'scala': 'data engineer',
    'cloud platforms': 'data engineer',
    'aws': 'data engineer',
    'azure': 'data engineer',
    'gcp': 'data engineer',
    'data engineer': 'data engineer',
    'data pipelines': 'data engineer',
    'batch processing': 'data engineer',
    'stream processing': 'data engineer',
    'data governance': 'data engineer',
    'data modeling': 'data engineer',
    'data architecture': 'data engineer',

    # Data Scientist
    'machine': 'data scientist',
    'learning': 'data scientist',
    'tensorflow': 'data scientist',
    'pytorch': 'data scientist',
    'nlp': 'data scientist',
    'statistics': 'data scientist',
    'models': 'data scientist',
    'ai': 'data scientist',
    'structured data': 'data scientist',
    'unstructured data': 'data scientist',
    'data analysis': 'data scientist',
    'visualization': 'data scientist',
    'tableau': 'data scientist',
    'matplotlib': 'data scientist',
    'r': 'data scientist',
    'data scientist': 'data scientist',
    'clustering': 'data scientist',
    'regression': 'data scientist',
    'classification': 'data scientist',
    'dimensionality reduction': 'data scientist',
    'time series': 'data scientist',
    'hypothesis testing': 'data scientist',

    # ML Engineer
    'ml': 'ml engineer',
    'deep': 'ml engineer',
    'neural': 'ml engineer',
    'training': 'ml engineer',
    'deployment': 'ml engineer',
    'model': 'ml engineer',
    'keras': 'ml engineer',
    'aws sagemaker': 'ml engineer',
    'gcp ai platform': 'ml engineer',
    'optimization': 'ml engineer',
    'machine learning': 'ml engineer',
    'artificial intelligence': 'ml engineer',
    'feature engineering': 'ml engineer',
    'model evaluation': 'ml engineer',
    'hyperparameter tuning': 'ml engineer',
    'mlops': 'ml engineer',
    'model explainability': 'ml engineer',
    'reinforcement learning': 'ml engineer',

    # Frontend Engineer
    'react': 'frontend engineer',
    'javascript': 'frontend engineer',
    'html': 'frontend engineer',
    'css': 'frontend engineer',
    'bootstrap': 'frontend engineer',
    'ui': 'frontend engineer',
    'ux': 'frontend engineer',
    'frontend': 'frontend engineer',
    'user interfaces': 'frontend engineer',
    'angular': 'frontend engineer',
    'responsive design': 'frontend engineer',
    'accessibility': 'frontend engineer',
    'design standards': 'frontend engineer',
    'vue.js': 'frontend engineer',
    'typescript': 'frontend engineer',
    'webpack': 'frontend engineer',
    'sass': 'frontend engineer',
    'tailwind css': 'frontend engineer',
    'animation libraries': 'frontend engineer',

    # Backend Engineer
    'backend': 'backend engineer',
    'node': 'backend engineer',
    'django': 'backend engineer',
    'flask': 'backend engineer',
    'express': 'backend engineer',
    'api': 'backend engineer',
    'database': 'backend engineer',
    'restful': 'backend engineer',
    'graphql': 'backend engineer',
    'system performance': 'backend engineer',
    'mysql': 'backend engineer',
    'mongodb': 'backend engineer',
    'server-side logic': 'backend engineer',
    'redis': 'backend engineer',
    'postgresql': 'backend engineer',
    'load balancing': 'backend engineer',
    'authentication': 'backend engineer',
    'authorization': 'backend engineer',
    'microservices': 'backend engineer',

    # Fullstack Engineer
    'fullstack': 'fullstack engineer',
    'mern': 'fullstack engineer',
    'mean': 'fullstack engineer',
    'integration': 'fullstack engineer',
    'web applications': 'fullstack engineer',
    'end-to-end solutions': 'fullstack engineer',
    'scalable systems': 'fullstack engineer',
    'devops': 'fullstack engineer',
    'graphql APIs': 'fullstack engineer',
    'socket.io': 'fullstack engineer',
    'jwt': 'fullstack engineer',
    'progressive web apps': 'fullstack engineer',
    'monorepo': 'fullstack engineer',
    'real-time systems': 'fullstack engineer',

    # Cloud Engineer
    'cloud': 'cloud engineer',
    'aws': 'cloud engineer',
    'azure': 'cloud engineer',
    'gcp': 'cloud engineer',
    'devops': 'cloud engineer',
    'terraform': 'cloud engineer',
    'infrastructure': 'cloud engineer',
    'kubernetes': 'cloud engineer',
    'docker': 'cloud engineer',
    'cloud infrastructure': 'cloud engineer',
    'automation': 'cloud engineer',
    'cost optimization': 'cloud engineer',
    'cloud-native': 'cloud engineer',
    'serverless': 'cloud engineer',
    'cloud monitoring': 'cloud engineer',
    'cost efficiency': 'cloud engineer',
    'high availability': 'cloud engineer',
    'disaster recovery': 'cloud engineer',

    # Network Engineer
    'network': 'network engineer',
    'routing': 'network engineer',
    'switching': 'network engineer',
    'firewalls': 'network engineer',
    'vpn': 'network engineer',
    'tcp': 'network engineer',
    'dns': 'network engineer',
    'ip': 'network engineer',
    'cisco routers': 'network engineer',
    'juniper devices': 'network engineer',
    'wireshark': 'network engineer',
    'computer networks': 'network engineer',
    'troubleshoot': 'network engineer',
    'packet tracing': 'network engineer',
    'load balancing': 'network engineer',
    'network security': 'network engineer',

    # Cybersecurity
    'cybersecurity': 'cybersecurity',
    'security': 'cybersecurity',
    'penetration': 'cybersecurity',
    'vulnerability': 'cybersecurity',
    'malware': 'cybersecurity',
    'encryption': 'cybersecurity',
    'firewall': 'cybersecurity',
    'compliance': 'cybersecurity',
    'incident': 'cybersecurity',
    'forensics': 'cybersecurity',
    'gdpr': 'cybersecurity',
    'iso 27001': 'cybersecurity',
    'nessus': 'cybersecurity',
    'splunk': 'cybersecurity',
    'incident response': 'cybersecurity',
    'malware prevention': 'cybersecurity',
    'encryption algorithms': 'cybersecurity',
    'identity access management': 'cybersecurity',
    'zero trust': 'cybersecurity',
    'ransomware': 'cybersecurity',

    # QA Engineer
    'qa': 'qa engineer',
    'testing': 'qa engineer',
    'automation': 'qa engineer',
    'selenium': 'qa engineer',
    'cypress': 'qa engineer',
    'regression': 'qa engineer',
    'junit': 'qa engineer',
    'pytest': 'qa engineer',
    'bugs': 'qa engineer',
    'quality assurance': 'qa engineer',
    'functional testing': 'qa engineer',
    'load testing': 'qa engineer',
    'agile': 'qa engineer',
    'sdlc': 'qa engineer',
    'manual testing': 'qa engineer',
    'performance testing': 'qa engineer',
    'integration testing': 'qa engineer',

    # DevOps Engineer
    'ci cd': 'devops engineer',
    'jenkins': 'devops engineer',
    'circleci': 'devops engineer',
    'gitlab ci': 'devops engineer',
    'ansible': 'devops engineer',
    'chef': 'devops engineer',
    'puppet': 'devops engineer',
    'monitoring': 'devops engineer',
    'prometheus': 'devops engineer',
    'grafana': 'devops engineer',
    'continuous integration': 'devops engineer',
    'continuous deployment': 'devops engineer',
    'infrastructure as code': 'devops engineer',
    'k8s': 'devops engineer',
    'helm': 'devops engineer',
    'ci pipelines': 'devops engineer',
    'orchestration': 'devops engineer',
    'observability': 'devops engineer',

    # Site Reliability Engineer
    'site reliability': 'site reliability engineer',
    'sre': 'site reliability engineer',
    'uptime': 'site reliability engineer',
    'sla': 'site reliability engineer',
    'slo': 'site reliability engineer',
    'error budgeting': 'site reliability engineer',
    'observability': 'site reliability engineer',
    'incident management': 'site reliability engineer',
    'scalability': 'site reliability engineer',
    'fault tolerance': 'site reliability engineer',
    'site availability': 'site reliability engineer',

    # Product Manager
    'product management': 'product manager',
    'roadmap': 'product manager',
    'stakeholder': 'product manager',
    'requirements gathering': 'product manager',
    'user stories': 'product manager',
    'agile methodologies': 'product manager',
    'scrum': 'product manager',
    'product lifecycle': 'product manager',
    'market research': 'product manager',
    'competitive analysis': 'product manager',
    'backlog': 'product manager',
    'feature prioritization': 'product manager',
    'customer journey': 'product manager',
    'stakeholder alignment': 'product manager',

    # Business Analyst
    'business analysis': 'business analyst',
    'requirements analysis': 'business analyst',
    'use cases': 'business analyst',
    'process modeling': 'business analyst',
    'data modeling': 'business analyst',
    'stakeholder management': 'business analyst',
    'business intelligence': 'business analyst',
    'power bi': 'business analyst',
    'gap analysis': 'business analyst',
    'business': 'business analyst',
    'data visualization': 'business analyst',
    'process optimization': 'business analyst',
    'kpi tracking': 'business analyst',

    # UI/UX Designer
    'wireframes': 'ui ux designer',
    'prototyping': 'ui ux designer',
    'figma': 'ui ux designer',
    'sketch': 'ui ux designer',
    'design systems': 'ui ux designer',
    'adobe xd': 'ui ux designer',
    'usability testing': 'ui ux designer',
    'user research': 'ui ux designer',
    'interaction design': 'ui ux designer',
    'user flows': 'ui ux designer',
    'ui': 'ui ux designer',
    'ux': 'ui ux designer',
    'accessibility standards': 'ui ux designer',
    'information architecture': 'ui ux designer'
}


def extract_job_role(person_data, job_roles):
    all_text = []
    all_text += person_data.get("Skills", [])
    for exp in person_data.get("Work Experience", []):
        all_text += exp.get("Description", [])
    all_text += person_data.get("Certification", [])

    combined_text = ' '.join(all_text).lower()
    words = re.findall(r'\w+', combined_text)

    job_count = {}
    for word in words:
        if word in job_roles:
            job_role = job_roles[word]
            job_count[job_role] = job_count.get(job_role, 0) + 1

    if job_count:
        return max(job_count, key=job_count.get)
    return "Unknown"

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
    text = re.sub(r"[^a-zA-Z0-9\s,:]", "", text)
    doc = nlp(text)
    cleaned_text = [token.text.lower() for token in doc if token.text.lower() not in STOP_WORDS]
    return " ".join(cleaned_text)

# Ekstraksi Informasi Pribadi
def extract_personal_info(raw_text):
    phone = re.search(r'(?:\(\+62\)|\+62|62|0)\s?8\d{1,2}(?:[-.\s]?\d{2,4}){2,3}', raw_text)
    email = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', raw_text)
    linkedin = re.search(r'(?:https?://)?(?:www\.)?linkedin\.com/\S+', raw_text)
    github = re.search(r'(?:https?://)?(?:www\.)?github\.com/\S+', raw_text)
    degree = re.search(r'Bachelor\s+(?:degree|of)\s+[A-Za-z\s]+', raw_text, re.IGNORECASE)

    lines = [line.strip() for line in raw_text.split("\n") if line.strip()]
    name = lines[0] if lines else None

    return {
        "Phone Number": phone.group(0) if phone else None,
        "Email": email.group(0) if email else None,
        "Github": github.group(0) if github else None,
        "LinkedIn": linkedin.group(0) if linkedin else None,
        "Degree": degree.group(0) if degree else None
    }

# Fungsi untuk menghitung durasi pengalaman kerja
def calculate_duration(start_date, end_date):
    date_format = "%b %Y"
    try:
        start = datetime.strptime(start_date, date_format)
        if end_date.lower() == "present":
            end = datetime.now()
        else:
            end = datetime.strptime(end_date, date_format)
        duration = (end.year - start.year) * 12 + (end.month - start.month)
        return round(duration / 12, 2)
    except Exception as e:
        print(f'Error parsing dates: {e}')
        return 0

# Ekstraksi Keterampilan
def extract_skills(cleaned_text):
    tech_skills = { 
        "Programming Languages": ["python", "java", "javascript", "typescript", "c\\+\\+", "c#", "ruby", "php", "swift", "kotlin", "go", "rust", "scala"],
        "Web Developer": ["html", "css", "react", "angular", "vue", "nodejs", "django", "flask", "spring", "laravel", "express", "graphql", "rest api"],
        "Mobile Development": ["android", "ios", "flutter", "react native", "kotlin", "swift", "xamarin", "ionic", "jetpack compose", "android studio", "dart"],
        "Cloud & DevOps": ["aws", "azure", "google cloud", "docker", "kubernetes", "jenkins", "terraform", "ansible", "cloud computing", "saas", "iaas", "paas"],
        "Data & AI": ["machine learning", "data science", "deep learning", "tensorflow", "pytorch", "pandas", "numpy", "scikit-learn", "data analysis", "big data", "spark", "hadoop", "sql", "nosql", "tableau", "artificial intelligence", "computer vision", "natural language processing","nlp","ocr", "seaborn", "data engineering", "data visualization", "r", "r studio"],
        "Design & UX": ["ui", "ux", "figma", "sketch", "adobe xd", "photoshop", "user interface", "user experience", "graphic design"],
        "Databases": ["mysql", "postgresql", "mongodb", "sqlite", "redis", "oracle", "cassandra", "firebase"],
        "Other Technologies": ["git", "linux", "blockchain", "cybersecurity", "network security", "agile", "scrum", "microservices", "serverless"]
    }

    all_skills = [skill for category in tech_skills.values() for skill in category]
    extracted_skills = [skill for skill in all_skills if re.search(r'\b' + re.escape(skill.lower()) + r'\b', cleaned_text.lower())]
    return sorted(set(extracted_skills))

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

# Ekstraksi Fitur
def extract_features(data):
    texts = []
    numerical_features = []
    scores = []

    for entry in data:
        for name, details in entry.items():
            skills = details.get("Skills", [])
            certifications = details.get("Certification", [])
            personal_info = details.get("Personal Info", {})
            work_experience = details.get("Work Experience", [])

            skills_score = min(len(skills) / 20, 1.0) * 100
            certification_score = min(len(certifications) / 10, 1.0) * 100
            degree = personal_info.get("Degree", " ")
            degree_map = {'highschool': 10, 'bachelor': 20, 'master': 30, 'phd': 40}
            degree_score = degree_map.get(degree.lower().split(" ")[0], 0) * 2.5
            total_experience = sum(calculate_duration(exp.get("Start Date", ""), exp.get("End Date", "")) for exp in work_experience)
            experience_score = min(total_experience / 5, 1.0) * 100

            numerical_features.append([skills_score, certification_score, degree_score, experience_score])

            combined_text = " ".join(skills + certifications)
            texts.append(combined_text)

            final_score = (0.3 * skills_score + 0.2 * certification_score +
                           0.2 * degree_score + 0.3 * experience_score)
            scores.append(final_score)

    return texts, numerical_features, scores

# Tokenisasi Teks
#TODO move to vertex ai
tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')

def tokenize_texts(texts):
    return tokenizer(texts, padding='max_length', truncation=True, max_length=128, return_tensors="tf")

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
    start = os.environ.get("FUNCTION_CV_DIR", None)

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

    # parsing
    extracted_text = extract_text_from_pdf(pdf_content)
    cleaned_text = clean_text(extracted_text)
    result = parse_cv_to_json(extracted_text, cleaned_text)
        # Extract features for ML model
    texts, numerical_features, scores = extract_features([result])
        # Tokenization for BERT
    tokenized_texts = tokenize_texts(texts)

    job_role = extract_job_role(result[list(result.keys())[0]], job_roles)
    parsed_input = {
        'cv': result,
        'jobRole': job_role,
        'input_ids': tokenized_texts['input_ids'].numpy().tolist(),
        'attention_mask': tokenized_texts['attention_mask'].numpy().tolist(),
        'numerical_features': numerical_features
    }


    new_filename = filename.replace('.pdf', '.json')
    upload_blob = bucket.blob(new_filename)
    upload_blob.upload_from_string(
        data=json.dumps(parsed_input, indent=4, ensure_ascii=False),
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
