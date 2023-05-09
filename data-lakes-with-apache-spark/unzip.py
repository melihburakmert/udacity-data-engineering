import zipfile as zf

def unzip_sample_data():
    # Unzip log data
    files = zf.ZipFile("data/log-data.zip", 'r')
    files.extractall('data/')
    files.close()

    # Unzip song data
    files = zf.ZipFile("data/song-data.zip", 'r')
    files.extractall('data/')
    files.close()

if __name__ == "__main__":
    unzip_sample_data()