import requests
import pandas as pd
from io import BytesIO
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, func
from sqlalchemy.orm import sessionmaker, declarative_base
import numpy as np

# Authentication and token retrieval
client_id = '836392f3-067e-4b9a-a30a-8ce01cdb6b3e'
client_secret = 'RuM8Q~wivKYVfCD-Fnm.4FMP4D7Zm8DIOV_8sah-'
tenant_id = '1741dc9c-7588-4dd2-a053-570e5674f3b8'
username = 'lokesh.k@gain-hub.com'
password = 'Gainloki14@2003'

# Token endpoint
token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'

# Parameters for the token request
data = {
    'grant_type': 'password',
    'client_id': client_id,
    'client_secret': client_secret,
    'scope': 'https://graph.microsoft.com/.default offline_access',
    'username': username,
    'password': password,
}

# Make the request to obtain the access token and refresh token
token_response = requests.post(token_url, data=data)

# Check if the request was successful
if token_response.status_code == 200:
    tokens = token_response.json()
    access_token = tokens.get('access_token')
else:
    raise Exception('Error fetching access token: {} - {}'.format(token_response.status_code, token_response.json()))

# FastAPI setup
app = FastAPI()

# Pydantic model for the request body
class FileRequest(BaseModel):
    folder_name: str
    file_name: str

# Define the database URL
db_url = 'postgresql://odoo:odoo@172.188.42.27:9832/dev'

# Create an engine and a session
engine = create_engine(db_url)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# Define ORM models
class CrmLeadForm(Base):
    __tablename__ = 'crm_lead_form'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    lead_id = Column(String, nullable=False)
    contact_name = Column(String)
    company_ids = Column(String)
    title = Column(Integer)  # This refers to the title_id from res_partner_title table
    mobile = Column(String)
    email = Column(String)
    street = Column(String)
    street2 = Column(String)
    zip = Column(String)
    state_id = Column(Integer)
    country_id = Column(Integer)
    campaign_id = Column(Integer)
    source_id = Column(Integer)
    city = Column(String)

# Define other ORM models...
class UtmCampaign(Base):
    __tablename__ = 'utm_campaign'
    id = Column(Integer, primary_key=True)
    name = Column(String)

class ResCountryState(Base):
    __tablename__ = 'res_country_state'
    id = Column(Integer, primary_key=True)
    name = Column(String)

class ResCountry(Base):
    __tablename__ = 'res_country'
    id = Column(Integer, primary_key=True)
    name = Column(String)

class UtmSource(Base):
    __tablename__ = 'utm_source'
    id = Column(Integer, primary_key=True)
    name = Column(String)

class PartnerTitle(Base):
    __tablename__ = 'res_partner_title'
    id = Column(Integer, primary_key=True)
    name = Column(String)

# Create the tables if they don't exist
Base.metadata.create_all(engine)

# Function definitions (Updated to handle None values)
def get_campaign_id_by_name(campaign_name):
    # Check if the campaign_name is None
    if not campaign_name:
        return None

    session = Session()
    campaign = session.query(UtmCampaign).filter(
        func.replace(func.lower(UtmCampaign.name), ' ', '') == campaign_name.replace(' ', '').lower()
    ).first()
    session.close()
    return campaign.id if campaign else None

def get_state_id_by_name(state_name):
    # Check if the state_name is None
    if not state_name:
        return None

    session = Session()
    state = session.query(ResCountryState).filter(
        func.replace(func.lower(ResCountryState.name), ' ', '') == state_name.replace(' ', '').lower()
    ).first()
    session.close()
    return state.id if state else None

def get_country_id_by_name(country_name):
    # Check if the country_name is None
    if not country_name:
        return None

    session = Session()
    country = session.query(ResCountry).filter(
        func.replace(func.lower(ResCountry.name), ' ', '') == country_name.replace(' ', '').lower()
    ).first()
    session.close()
    return country.id if country else None

def get_source_id_by_name(source_name):
    # Check if the source_name is None
    if not source_name:
        return None

    session = Session()
    source = session.query(UtmSource).filter(
        func.replace(func.lower(UtmSource.name), ' ', '') == source_name.replace(' ', '').lower()
    ).first()
    session.close()
    return source.id if source else None

def get_title_id_by_name(title_name):
    # Check if the title_name is None
    if not title_name:
        return None

    session = Session()
    title = session.query(PartnerTitle).filter(
        func.replace(func.lower(PartnerTitle.name), ' ', '') == title_name.replace(' ', '').lower()
    ).first()
    session.close()
    return title.id if title else None

# Function to search shared folder (remains the same)
def search_shared_folder(access_token, folder_name, file_name):
    url = "https://graph.microsoft.com/v1.0/me/drive/sharedWithMe"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        found_folders = []

        for item in data.get("value", []):
            if item.get("folder") and item.get("name") == folder_name:
                found_folders.append(item)

        if found_folders:
            for folder in found_folders:
                site_id = folder.get("remoteItem", {}).get("parentReference", {}).get("siteId")
                item_id = folder.get("remoteItem", {}).get("id")

                children_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{item_id}/children"
                children_response = requests.get(children_url, headers=headers)

                if children_response.status_code == 200:
                    children_data = children_response.json()

                    for child in children_data.get("value", []):
                        if child['name'] == file_name:
                            if child['name'].endswith('.xlsx'):
                                file_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{child['id']}/content"
                                excel_response = requests.get(file_url, headers=headers)

                                if excel_response.status_code == 200:
                                    return BytesIO(excel_response.content)
                                else:
                                    print(f"Error accessing file: {excel_response.status_code} - {excel_response.text}")
                            else:
                                print("The file is not an Excel file.")
                            break
                    else:
                        print("File not found in the folder.")
                else:
                    print(f"Error retrieving children: {children_response.status_code} - {children_response.text}")
        else:
            print("No folders found with the specified name.")
    else:
        print(f"Error: {response.status_code} - {response.text}")

    return None

# FastAPI endpoint
@app.post("/search_folder/")
def search_folder(request: FileRequest):
    global access_token
    excel_content = search_shared_folder(access_token, request.folder_name, request.file_name)
    
    if excel_content:
        df = pd.read_excel(excel_content)

        df = df.apply(lambda x: x.map(lambda y: None if isinstance(y, float) and (np.isinf(y) or np.isnan(y)) else y))

        with Session() as session:
            for index, row in df.iterrows():
                lead_id = row.get('lead_id')
                existing_lead = session.query(CrmLeadForm).filter(CrmLeadForm.lead_id == lead_id).first()
                if not existing_lead:
                    campaign_id = get_campaign_id_by_name(row.get('campaign_name')) if 'campaign_name' in row else None
                    state_id = get_state_id_by_name(row.get('state_name')) if 'state_name' in row else None
                    country_id = get_country_id_by_name(row.get('country_name')) if 'country_name' in row else None
                    source_id = get_source_id_by_name(row.get('source_name')) if 'source_name' in row else None
                    title_id = get_title_id_by_name(row.get('title_name')) if 'title_name' in row else None

                    new_lead = CrmLeadForm(
                        lead_id=lead_id,
                        contact_name=row.get('contact_name'),
                        company_ids=row.get('company_ids'),
                        title=title_id,
                        mobile=row.get('mobile'),
                        email=row.get('email'),
                        street=row.get('street'),
                        street2=row.get('street2'),
                        zip=row.get('zip'),
                        state_id=state_id,
                        country_id=country_id,
                        campaign_id=campaign_id,
                        source_id=source_id,
                        city=row.get('city'),
                    )
                    session.add(new_lead)
            session.commit()

        return {"message": "Data processed successfully"}
    else:
        raise HTTPException(status_code=404, detail="File not found.")
