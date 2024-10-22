import calendar

def get_day_previous_month(year, month):
    if month == 1:
        previous_year = year - 1
        previous_month = 12
    else:
        previous_year = year
        previous_month = month - 1

    # Obtener el último día del mes anterior
    previous_day = calendar.monthrange(previous_year, previous_month)[1]

    return previous_day, previous_month, previous_year

def get_next_month(year, month):
    if month == 12:
        next_year = year + 1
        next_month = 1
    else:
        next_year = year
        next_month = month + 1

    return next_month, next_year