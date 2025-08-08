# phonepe-dashboard
A Streamlit-based PhonePe transactions dashboard

India’s digital payments journey has amazed the world. From big cities to small villages, mobile phones, internet access, and advanced payment systems built by the government and central bank have fueled a payments revolution. PhonePe, launched in 2016, has grown rapidly thanks to India’s API-driven digital payment infrastructure. In the early days, we struggled to find clear, reliable data on digital payments in India.


Extracting Year-wise and State-wise Data from a JSON File in GitHub:
1. Accessing the Dataset from GitHub
  The dataset is stored in JSON format inside a GitHub repository.
  To work with it, the first step is to clone the repository to your local machine or open it in an environment like Google Colab.
  Git command to clone: git clone https://github.com/UserName/RepositoryName.git
  This creates a local copy of the repository containing the JSON files.

2. Understanding the JSON File
JSON (JavaScript Object Notation) is a lightweight data format that stores data in key-value pairs.
In the PhonePe dataset, the JSON file contains hierarchical data, typically structured as:
Year → Quarter → State → Data values (e.g., transactions, user counts, etc.)
Example snippet:
json
Copy
Edit
{
  "year": 2018,
  "state": "Karnataka",
  "transaction_count": 123456,
  "transaction_amount": 7890000
}

3. Opening the File in VS Code or Colab
In VS Code:
Open the folder where you cloned the GitHub repo.
Locate the .json file and open it to view raw data.

In Google Colab:
Either upload the JSON file manually or mount your GitHub repo/Google Drive.
Use Python’s open() and json module to read the data:
python
Copy
Edit
import json
with open('data.json', 'r') as file:
    data = json.load(file)


