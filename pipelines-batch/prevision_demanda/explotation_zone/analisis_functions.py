def read_dataset(spark, file_path):
    df = spark.read.csv(file_path, header=True, inferSchema=True, sep=",")
    
    return df

def save_dataset(df, file_path):
     df.coalesce(1).write.mode("overwrite").csv(file_path, header=True)

def categorize_hour(hour):
    if hour in [0, 1, 2, 3, 4, 5]:
        return 'night'
    elif hour in [6, 7, 8, 9]:
        return 'early_morning'
    elif hour in [10, 11, 12, 13, 14]:
        return 'morning'
    elif hour in [15, 16, 17, 18, 19]:
        return 'afternoon'
    elif hour in [20, 21, 22, 23]:
        return 'evening'
    else:
        return None
    
def get_season(date):
    day = date.day
    month = date.month

    if (month == 3 and day >= 21) or (3 < month < 6) or (month == 6 and day < 21):
        return 'Primavera'
    elif (month == 6 and day >= 21) or (6 < month < 9) or (month == 9 and day < 23):
        return 'Verano'
    elif (month == 9 and day >= 23) or (9 < month < 12) or (month == 12 and day < 21):
        return 'Otoño'
    else:
        return 'Invierno'