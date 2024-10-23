from dotenv import load_dotenv
import calendar


load_dotenv()

def check_file_existence_hdfs(spark, hdfs_path):
    # Check if the path already exists
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI.create(hdfs_path),
                                                        spark._jsc.hadoopConfiguration(), )
    path_exists = fs.exists(spark._jvm.org.apache.hadoop.fs.Path(hdfs_path))
    return path_exists


def list_files_by_condition (hdfs_path, fs, Path, condition='format'):
    file_status = fs.listStatus(Path(hdfs_path))
    files = []
    for status in file_status:
            path_dir = status.getPath().toString()
            if condition == 'format':
                if path_dir.endswith(".csv") or path_dir.endswith(".json"):
                  files.append(status.getPath().toString())
                else:
                  files.extend(list_files_by_condition(path_dir,fs,Path, condition))
            if condition == 'partition':
                if 'month=' in path_dir:
                  files.append(status.getPath().toString())
                else:
                  files.extend(list_files_by_condition(path_dir,fs,Path, condition))
    return files

def get_HDFS_FileSystem(sc):
    URI = sc._gateway.jvm.java.net.URI
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration

    fs = FileSystem.get(URI("hdfs://hadooop-hadoop-hdfs-nn:9000"), Configuration())
    return fs

def get_FileList_From_HDFS_Path(fs, Path, hdfs_path):
    file_status = fs.listStatus(Path(hdfs_path))
    files = []
    for status in file_status:
        file_path = status.getPath().toString()
        if status.isDir() and (file_path.endswith(".json") or file_path.endswith(".csv")):
            files.append(status.getPath().toString())
        else:
            files.extend(get_FileList_From_HDFS_Path(fs,Path,hdfs_path=status.getPath().toString()))

    return files

def delete_file_hdfs(spark, hdfs_path):

    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI.create(hdfs_path),
                                                        spark._jsc.hadoopConfiguration(), )
    return fs.delete(spark._jvm.org.apache.hadoop.fs.Path(hdfs_path), True)


def get_day_previous_month(year, month):
    if month == 1:
        previous_year = year - 1
        previous_month = 12
    else:
        previous_year = year
        previous_month = month - 1

    # Obtener el último día del mes anterior
    previous_day = calendar.monthrange(previous_year, previous_month)[1]

    return previous_day, previous_month, previous_year

def get_next_month(year, month):
    if month == 12:
        next_year = year + 1
        next_month = 1
    else:
        next_year = year
        next_month = month + 1

    return next_month, next_year
