import os
import ast

def extract_urlsnolist_from_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        try:
            tree = ast.parse(file.read(), filename=file_path)
            for node in ast.walk(tree):
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Name) and target.id == 'URLSNoList':
                            if isinstance(node.value, (ast.List, ast.Tuple)):
                                return ast.literal_eval(node.value)
        except Exception as e:
            print(f"Error parsing {file_path}: {e}")
    return None

def extract_urlsnolist_from_directory(directory):
    results = {}
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                urlsnolist_value = extract_urlsnolist_from_file(file_path)
                if urlsnolist_value is not None:
                    results[file_path] = urlsnolist_value
    return results

if __name__ == '__main__':
    directory = input("Enter the directory path: ")
    urlsnolist_values = extract_urlsnolist_from_directory(directory)
    for file, value in urlsnolist_values.items():
        print(value[0])
