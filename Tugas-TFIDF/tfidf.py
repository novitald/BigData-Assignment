import argparse
from pyspark import SparkConf, SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF

#Buat objek parser untuk menerima keyword dari argumen command-line
parser = argparse.ArgumentParser()
parser.add_argument('keyword', metavar='KEYWORD', help='keyword pencarian')
args = parser.parse_args()

conf = SparkConf().setMaster("local[*]").setAppName("SparkTFIDF")
sc = SparkContext(conf = conf)


# Baca dokumen.
rawData = sc.textFile("C:/Users/Wik/Documents/Kuliah/BigData/Tugas-3/dataset/dataset.tsv")
fields = rawData.map(lambda x: x.split("\t"))
# Supaya case-insensitive
# Ubah huruf-huruf pada baris menjadi huruf kecil dengan menggunakan fungsi lower() sebelum di-split
documents = fields.map(lambda x: x[1].lower().split(" "))

documentId = fields.map(lambda x: x[0])

# Buat tabel tf
hashingTF = HashingTF(100000)
tf = hashingTF.transform(documents)

# Buat objek idf
tf.cache()
idf = IDF(minDocFreq=1).fit(tf)

# Hitung tfidf
tfidf = idf.transform(tf)

# Ubah huruf-huruf pada keyword menjadi huruf kecil lalu hitung nilai hash-nya
keywordTF = hashingTF.transform([args.keyword.lower()])
keywordHashValue = int(keywordTF.indices[0])

# Temukan relevansinya dengan tabel tf-idf yang sudah dibuat
keywordRelevance = tfidf.map(lambda x: x[keywordHashValue])

# Zip dengan RDD documentId untuk mendapatkan ID masing-masing dokumen
zippedResults = keywordRelevance.zip(documentId)

# Ambil dokumen yang paling relevan
relevantDoc = zippedResults.max()

# Kalau keyword tidak ditemukan di dokumen manapun
if (relevantDoc[0] == 0):
   print "Maaf, keyword \"" + args.keyword + "\" tidak ditemukan di dokumen manapun"
# Jika ada dokumen yang relevan, tampilkan ID-nya
else:
   print "ID Dokumen yang relevan dengan kata kunci \"" + args.keyword + "\" : " + str(relevantDoc[1])