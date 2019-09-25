import os
folder = '/Users/bhavani.sankar/Desktop/Proj/Asurion/Tasks/Homma/20_file_size_computation_RAS_Activity_appuse_logs/s3_parquet/s3_parquet_data_201905/'
folder_size = 0
parquet_file_size_list = []
for (path, dirs, files) in os.walk(folder):
  for file in files:
    filename = os.path.join(path, file)
    filesize = os.path.getsize(filename)/1024
    #print(path,filesize)
    parquet_file_size_list.append((path,filesize))
    #folder_size += os.path.getsize(filename)
#print("Folder = %0.1f MB" % (folder_size/(1024*1024.0)))
#print("Folder = %0.1f MB" % (folder_size/(1024*1024)))
print(len(parquet_file_size_list))
print(type(parquet_file_size_list))
print(parquet_file_size_list)
