import logging
import os
from google.cloud import storage
from acesso import *
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials pls"

bucket_name = "bucket orign"
new_bucket_name ="bucket_backcup" 
file_path="path_to_files"


bucket = get_bucket(bucket_name)
if bucket: 
    get_bucket_details(bucket)
    lista=list_files(bucket)
    print("arquivos encotrados")
    for i in lista:
        print(i)

    lista2 = processesar_lista(lista)
    print("iniciando movimentação de arquivos para local")
    resultado = download_all_files(bucket,lista,lista2,file_path)
    print("sucessos de :",(len(resultado[0])),"/",(len(lista)))
    for  i in resultado[0]:
        print(i)


    if len(resultado[1]) < 1:
        print("sem erros")
    else: 
        print("falhas de",(len(resultado[0])),"/",(len(lista)))
        for j in resultado[1]:
            print(j)
    new_bucket = get_bucket(new_bucket_name)
    if not new_bucket :
        new_bucket = create_backup(new_bucket_name)

    lista = resultado[0]
    lista2 = processesar_lista(lista)
    print("enviando")
    resultado=upload_all_files(new_bucket,lista,lista2,file_path)

    print("sucessos de :",(len(resultado[0])),"/",(len(lista)))
    for  i in resultado[0]:
        print(i)
    if len(resultado[1]) < 1:
        print("sem erros")
    else: 
        print("falhas de",(len(resultado[0])),"/",(len(lista)))
        for j in resultado[1]:
            print(j)
