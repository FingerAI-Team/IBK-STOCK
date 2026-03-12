from dataclasses import dataclass
from typing import Any

@dataclass 
class LogOutput:
    '''
    val[0]: {'tenant_id': 'ibk', 'user_id': 'user123', 'date': '2024-06-01T12:00:00Z', 'Q': 'What is the interest rate?', 'A': 'The interest rate is 3.5%.'}
    '''
    val: list[dict[str, Any]]