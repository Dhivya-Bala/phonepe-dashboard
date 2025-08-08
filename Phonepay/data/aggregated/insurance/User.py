import pandas as pd
import json
import os

# Set path to data Aggregated User
path = r"C:\Users\DhivyaBharthi\Desktop\GUVI\Phonepay\data\aggregated\user\country\india\state"
user_state_list = os.listdir(path)

#  Define the dictionary OUTSIDE the loop
user = {
    'State': [],
    'Year': [],
    'Quater': [],
    'Brand_type': [],
    'Registered_count': [],
    'Percentage_Userdevices': []
}

# Loop through all JSON files and extract data
for i in user_state_list:
   p_i = os.path.join(path, i)
   user_yr = os.listdir(p_i)

   for j in user_yr:
       p_j = os.path.join(p_i, j)
       user_yr_list = os.listdir(p_j)

       for k in user_yr_list:
            p_k = os.path.join(p_j, k)
            with open(p_k, 'r') as Data:
                D = json.load(Data)

                if D['data']['usersByDevice'] is not None:
                    for z in D['data']['usersByDevice']:
                        Name = z.get('brand', 'Unknown')
                        count = z.get('count', 0)
                        Percentage = z.get('percentage', 0.0)

                        user['Brand_type'].append(Name)
                        user['Registered_count'].append(count)
                        user['Percentage_Userdevices'].append(Percentage)
                        user['State'].append(i)
                        user['Year'].append(j)
                        user['Quater'].append(int(k.strip('.json')))

#  Create DataFrame
Agg_user = pd.DataFrame(user)
Agg_user.shape

# Optional: Preview the data
print(Agg_user.head())