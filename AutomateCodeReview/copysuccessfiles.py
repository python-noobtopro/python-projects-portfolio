import os
import shutil
import glob

def copy_python_files(file_list, input_folder, output_folder):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"Created output folder: {output_folder}")
    
    not_found_files = []
    
    for file_prefix in file_list:
        # Use glob to find files starting with the prefix and ending with .py
        matching_files = glob.glob(os.path.join(input_folder, f"{file_prefix}*.py"))
        # print(matching_files)
        if matching_files:
            for input_file_path in matching_files:
                file_name = os.path.basename(input_file_path)
                output_file_path = os.path.join(output_folder, file_name)
               
                # Check if the file already exists in the destination folder
                if not os.path.exists(output_file_path):
                    shutil.copy(input_file_path, output_file_path)
                    os.remove(input_file_path)

                    print(f"Copied: {file_name} to {output_folder}")
                else:
                    print(f"Skipped: {file_name} (already exists in {output_folder})")
        else:
            not_found_files.append(file_prefix)
    

if __name__ == '__main__':
    file_list = ['2000321','4257790','5545660_1437625','5545660_1437627','5563814','1379814_1439963','1379814_1439964','1423949','1441158','2464492','2959389','3028003','3713248','1880547','4664338','1203437','1745664','1591671','1090331','1128657','1716231','1912114','2783786','2946468','3576100','1608112_1438676','2161131','1593513','1600439','2304633','1203437','1264941','1369533','1389321','1405201','1429899','1447125','1498869','1542331_1446371','1566540','1600439','1757819','1810782','1834104','1879020','1993043','1993636','2001046','2019363','2039602','2079183','2099846','2143619','2149265','2176107','2346937','2382881','2416311','2864543','2946468','2957181','2959389','3031738','3066416','3398524','3400868','3557767','3576100','3583365','3690968','3727390','3850398','3851136','4503026','5430144','5490960','5544240','5558876','5561309','5561527','4674086_1441052','1039122','1072060','1147301','1335988','1372677','1384092','1411372','1518464','1528141','1552651','1556844','1605402','1691345','1880547_1446420','1894501','2126479','2355088','2455407','2565007','2803418','3086959','3713248','3849650','3857749','3976451','4418904_1446487','4475358','5182097','5486755','5548034','5551734','5570526','4674086_1441069','2477096']

    input_folder = "C:\\Users\\Rupesh.Ranjan\\OneDrive - GlobalData PLC\\Desktop\\Nexgile_Tasks\\scheduled_200325_old"
    output_folder = "C:\\Users\\Rupesh.Ranjan\\OneDrive - GlobalData PLC\\Desktop\\Nexgile_Tasks\\scheduled_200325_old_NonTier1"
    
    copy_python_files(file_list, input_folder, output_folder)
