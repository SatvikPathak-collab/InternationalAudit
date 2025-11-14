excluded_conditions_preauth = {
    'eq_dict': 
    {
        'CORPORATE_NAME': {
            "MINISTRY OF FOREIGN AFFAIRS": {"consultation", "pharmacy", "investigation"}
        }
    },
    'eq':
    {
        'VIP_YN': {'Y'},
        'BENEFIT_TYPE': {'in-patient maternity', 'in-patient', 'day care'},
        'PA_STATUS': {'cancelled'}
    }
}

excluded_conditions_claims = {
    'eq_dict': 
    {
        'PROVIDER_NAME': {
            "AL AHLI HOSPITAL" : {"consultation", "pharmacy", "investigation"}, 
            "AL EMADI OPTICS": {"consultation", "investigation"}, 
            "AL EMADI HOSPITAL CLINICS - NORTH": {"consultation", "investigation"}, 
            "AL EMADI HOSPITAL": {"consultation", "investigation"}
        },
        'CORPORATE_NAME': {
            "MINISTRY OF FOREIGN AFFAIRS": {"consultation", "pharmacy", "investigation"}
        }
    },
    'eq': 
    {
        'BENEFIT_TYPE': {'in-patient maternity', 'in-patient', 'day care'},
        'SUBMISSION_TYPE': {'re-submission'}
    },
    'not_na':
    [
        'PRE_AUTH_NUMBER'
    ]
}