import requests
import logging

def send_audit_data(webhook_url, my_asin, first_comp, second_comp):
    """
    Sends extracted data to n8n webhook via GET request.
    Query params: myListing, firstComp, secondComp
    Returns: (success: bool, response_data: dict)
    """ 
    if not webhook_url:
        logging.warning("No Webhook URL provided. Skipping dispatch.")
        return False, None

    params = {
        "myListing": my_asin,
        "firstComp": first_comp,
        "secondComp": second_comp
    }

    try:
        logging.info(f"Sending Webhook to: {webhook_url}")
        logging.info(f"Payload: {params}")
        
        response = requests.get(webhook_url, params=params)
        
        if response.status_code == 200:
            logging.info(f"Webhook Success! Response: {response.text}")
            try:
                response_data = response.json()
                return True, response_data
            except:
                return True, None
        else:
            logging.error(f"Webhook Failed. Status: {response.status_code}, Response: {response.text}")
            return False, None
            
    except Exception as e:
        logging.error(f"Error sending webhook: {e}")
        return False, None
