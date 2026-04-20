import requests
import os

def get_contact_info(company_name):
    api_key = os.getenv("APOLLO_API_KEY")
    url = "https://api.apollo.io/v1/people/search"
    
    # We look for Decision Makers (CEO, Director, Owner)
    payload = {
        "api_key": api_key,
        "q_organization_name": company_name,
        "person_titles": ["CEO", "Director General", "Gerente", "Jefe de Obra", "Owner"]
    }
    
    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        data = response.json()
        
        if data.get("people"):
            person = data["people"][0] # Take the first match
            return {
                "name": person.get("name"),
                "title": person.get("title"),
                "email": person.get("email"),
                "linkedin": person.get("linkedin_url")
            }
    except Exception as e:
        print(f"Error in Apollo Discovery: {e}")
    
    return None
