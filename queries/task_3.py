# Exported Pipeline To Python
[
    {
        '$group': {
            '_id': {
                '$dateToString': {
                    'format': '%Y-%m-%d', 
                    'date': {
                        '$toDate': '$at'
                    }
                }
            }, 
            'avgRating': {
                '$avg': '$score'
            }
        }
    }
]