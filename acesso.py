from google.cloud import storage
from google.cloud import client
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\lucas\\OneDrive\\Documentos\\scripts\\fort\\pyhton adm\\eloquent-ratio-406010-3273ba76b74d.json"

def create_backup(backup_name):# cria o bucket de backup
    bucket = storage.Client().bucket(backup_name)
    bucket.location = 'us'
    bucket = storage.Client.create_bucket(bucket)
    return bucket

def get_bucket_details(bucket):
    print(vars(bucket))

def get_bucket(bucket_name):
    try: 
        bucket = storage.Client()
        bucket = bucket.get_bucket(bucket_name)
        return bucket
    except Exception as e:
        return  False
   

def upload_to_bucket(blob_name,file_path,bucket):#blob = referencia binaria para arquivo , blob_name =  dirarquivo , filepath = r'cdirfile'
    try:
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(file_path)
        return True 
    except Exception as e:
        print(e)
        return  False

def download_files(blob_name,file_path,file_name,bucket):
    try:
        if not (os.path.isdir(file_path)):
            os.makedirs(file_path)
        blob= bucket.blob(blob_name)
        file= file_path + '/' + file_name
        blob.download_to_filename(file)
        #with open(file_path,'wb') as f:
         #   bucket.download_blob_to_file(blob,f)
        return True
    except Exception as e:
        print(e)
        return False
    
def list_files(bucket):
    storge = storage.Client()
    blobs = storge.list_blobs(bucket)
    lista = []
    for blob in blobs:
        lista.append(blob.name)
    lista.sort
    return lista

def processesar_lista(lista):
    path = ""
    lista2 = []
    for i in lista:
        path = [] # anota o caminho no bucket
        i = i.split('/') # separa as pastas do nome do arquivo 
        path.append(i[0]) # salva o nome da pasta posição [0]
        i = i[1].split('.') #separa o nome do arquivo da extenção no proprio i
        path.append(i[0]) #add nome do arquivo pos [1]
        path.append(i[1]) # and nome extenção do arquivo na posição [2]
        lista2.append(path)
    return lista2


def download_all_files(bucket,lista,lista2,dir):
    sucesso =  []
    fracasso = []
    
    for i  in range( 0,len(lista) ):
        path = lista2[i][0]
        path = dir+"/"+path
        nome = lista2[i][1]+"."+lista2[i][2]
        if download_files(lista[i],path,nome,bucket):
            sucesso.append(lista[i])
        else:
            fracasso.append(lista[i])
    return [sucesso,fracasso]

def upload_all_files(bucket,lista,lista2,dir):
    sucesso = []
    fracasso = []

    for i in range (0,len(lista)):
        try: 
            blod_dir = lista2[i][0]
            path = dir+"/"+blod_dir
            nome = lista2[i][1]+"."+lista2[i][2]
            path = path+"/"+nome
            blob_name = blod_dir+"/"+lista2[i][1]+"/"+nome
            sucesso.append(lista[i])
            upload_to_bucket(blob_name,path,bucket)#blob_name,file_path,bucket
        except Exception as e:
            print(e)
            fracasso.append(lista[i])
    return [sucesso,fracasso]
       