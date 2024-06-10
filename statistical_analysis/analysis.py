def avg_emission_by_year(input_data): 
    sub_data = input_data.drop(input_data[0:2], axis=1)
    avg_emision = sub_data.groupby('Year').mean()