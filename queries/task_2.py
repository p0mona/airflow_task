# Exported Pipeline To Python
[
    {
        '$match': {
            '$expr': {
                '$lt': [
                    {
                        '$strLenCP': '$content'
                    }, 5
                ]
            }
        }
    }
]