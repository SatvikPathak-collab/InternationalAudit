excluded_conditions_preauth = {
    'not_eq_dict': 
    {
        'CORPORATE_NAME': {
            "MINISTRY OF FOREIGN AFFAIRS": {"invalid"}
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
    'not_eq_dict': 
    {
        'PROVIDER_NAME': {
            "AL AHLI HOSPITAL" : {"invalid"}, 
            "AL EMADI OPTICS": {"pharmacy"}, 
            "AL EMADI HOSPITAL CLINICS - NORTH": {"pharmacy"}, 
            "AL EMADI HOSPITAL": {"pharmacy"}
        },
        'CORPORATE_NAME': {
            "MINISTRY OF FOREIGN AFFAIRS": {"invalid"}
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