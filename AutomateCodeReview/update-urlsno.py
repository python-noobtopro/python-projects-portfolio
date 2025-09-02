import pandas as pd
import os
import re

def update_script_from_excel(excel_file, script_folder):
    # Load the Excel file
    df = pd.read_excel(excel_file)
    
    for index, row in df.iterrows():
        script_filename = row['FileName']
        new_urlsno = int(row['URLSNoList'])
        script_path = os.path.join(script_folder, f"{script_filename}.py")
        
        if os.path.exists(script_path):
            with open(script_path, 'r', encoding='utf-8') as file:
                script_content = file.read()
            
            # Replace the existing URLSNoList dynamically
            updated_content = re.sub(r'URLSNoList\s*=\s*\[\d+(\.\d+)?\]', f'URLSNoList = [{new_urlsno}]', script_content)
            
            with open(script_path, 'w', encoding='utf-8') as file:
                file.write(updated_content)
            
            print(f"Updated {script_filename} with URLSNo {new_urlsno}")
        else:
            # print(script_filename)
            print(f"File {script_filename} not found in {script_folder}")

# Example usage
excel_file = "C:\\Users\\Rupesh.Ranjan\\OneDrive - GlobalData PLC\\Desktop\\Nexgile_Tasks\\jobsgenericwebcrawlerapp_new_template\\mapping_23042025.xlsx"  # Update with your actual Excel file path
script_folder = "C:\\Users\\Rupesh.Ranjan\\OneDrive - GlobalData PLC\\Desktop\\Nexgile_Tasks\\jobsgenericwebcrawlerapp_new_template\\23_04_2025"  # Folder where Python scripts are stored
update_script_from_excel(excel_file, script_folder)
