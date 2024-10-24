from functions import read_estado_bicing, format_data_estado_bicing, forwardfill_availability, calculate_tasa_eficiencia_ajustada, save_estado_bicing

def calculo_tasa_eficiencia():
    estado_bicing_2021 = read_estado_bicing("data/estado_bicing_2021.csv")
    estado_bicing_2022 = read_estado_bicing("data/estado_bicing_2022.csv")
    estado_bicing_2023 = read_estado_bicing("data/estado_bicing_2023.csv")
    estado_bicing_2024 = read_estado_bicing("data/estado_bicing_2024.csv")

    estado_bicing_2021 = format_data_estado_bicing(estado_bicing_2021)
    estado_bicing_2022 = format_data_estado_bicing(estado_bicing_2022)
    estado_bicing_2023 = format_data_estado_bicing(estado_bicing_2023)
    estado_bicing_2024 = format_data_estado_bicing(estado_bicing_2024)

    estado_bicing_2021 = forwardfill_availability(estado_bicing_2021)
    estado_bicing_2022 = forwardfill_availability(estado_bicing_2022)
    estado_bicing_2023 = forwardfill_availability(estado_bicing_2023)
    estado_bicing_2024 = forwardfill_availability(estado_bicing_2024)

    estado_bicing_2021 = calculate_tasa_eficiencia_ajustada(estado_bicing_2021)
    estado_bicing_2022 = calculate_tasa_eficiencia_ajustada(estado_bicing_2022)
    estado_bicing_2023 = calculate_tasa_eficiencia_ajustada(estado_bicing_2023)
    estado_bicing_2024 = calculate_tasa_eficiencia_ajustada(estado_bicing_2024)

    save_estado_bicing(estado_bicing_2021, "data/estado_bicing_limpio_2021.csv")
    save_estado_bicing(estado_bicing_2022, "data/estado_bicing_limpio_2022.csv")
    save_estado_bicing(estado_bicing_2023, "data/estado_bicing_limpio_2023.csv")
    save_estado_bicing(estado_bicing_2024, "data/estado_bicing_limpio_2024.csv")

if __name__ == "__main__":
    calculo_tasa_eficiencia()