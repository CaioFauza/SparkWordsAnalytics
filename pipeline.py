from pyspark import SparkContext
import math
import pandas as pd

def document_counter(item):
    words_brute = item[1].strip().split()
    words = [i.lower() for i in words_brute if (not any(j.isdigit() for j in i) and len(i) > 3 and (not("——") in i) and (not ("(") in i) and (not (")") in i) and (not ("www") in i))]
    
    if(scope == 0):
        if("hyundai" in words and "honda" in words):
            return [(i, 1) for i in set(words)]
    elif(scope == 1):
        if("hyundai" in words and "honda" not in words):
            return [(i, 1) for i in set(words)]
    else:
        if("hyundai" not in words and "honda" in words):
            return [(i, 1) for i in set(words)]
    return []

def word_counter(item):
    words_brute = item[1].strip().split()
    words = [i.lower() for i in words_brute if (not any(j.isdigit() for j in i) and len(i) > 3 and (not("——") in i) and (not ("(") in i) and (not (")") in i) and (not ("www") in i))]
    
    if(scope == 0):
        if("hyundai" in words and "honda" in words):
            return [(i, 1) for i in words]
    elif(scope == 1):
        if("hyundai" in words and "honda" not in words):
            return [(i, 1) for i in words]
    else:
        if("hyundai" not in words and "honda" in words):
            return [(i, 1) for i in words]
    return []

def count_words(result ,item):
    return result+item

def idf(item):
    return item[0], math.log(N/item[1],10)
        
def freq(item):
    return item[0], math.log(1 + item[1], 10)

def relevancy(item):
    return item[0], item[1][0] * item[1][1]

if __name__ == '__main__':
    scope = 0
    N = 0
    sc = SparkContext(appName='project')
    rdd = sc.sequenceFile("s3://megadados-alunos/web-brasil")

    # Variable scope -> 0 = Words together, 1 = Hyundai alone, 2 = Honda alone
    scope = 0
    docs_together = rdd.flatMap(document_counter).reduceByKey(count_words)
    words_together = rdd.flatMap(word_counter).reduceByKey(count_words)

    scope = 1
    docs_hyundai = rdd.flatMap(document_counter).reduceByKey(count_words)
    words_hyundai = rdd.flatMap(word_counter).reduceByKey(count_words)

    scope = 2
    docs_honda = rdd.flatMap(document_counter).reduceByKey(count_words)
    words_honda = rdd.flatMap(word_counter).reduceByKey(count_words)

    N = docs_together.count()
    rdd_idf_together = docs_together.map(idf)
    
    N = docs_hyundai.count()
    rdd_idf_hyundai = docs_hyundai.map(idf)

    N = docs_honda.count()
    rdd_idf_honda = docs_honda.map(idf)

    rdd_freq_together = words_together.map(freq)
    rdd_freq_hyundai = words_hyundai.map(freq)
    rdd_freq_honda = words_honda.map(freq)

    rdd_relevancy_together = rdd_idf_together.join(rdd_freq_together).map(relevancy)
    rdd_relevancy_hyundai = rdd_idf_hyundai.join(rdd_freq_hyundai).map(relevancy)
    rdd_relevancy_honda = rdd_idf_honda.join(rdd_freq_honda).map(relevancy)
    
    top_100_together = rdd_relevancy_together.takeOrdered(100, key=(lambda x: -x[1]))
    result_together = pd.DataFrame(top_100_together, columns=(['word', 'relevancy']))
    result_together.to_csv(f"s3://megadados-alunos/caio-pedro/tables/together.csv")

    top_100_hyundai = rdd_relevancy_hyundai.takeOrdered(100, key=(lambda x: -x[1]))
    result_hyundai = pd.DataFrame(top_100_hyundai, columns=(['word', 'relevancy']))
    result_hyundai.to_csv(f"s3://megadados-alunos/caio-pedro/tables/hyundai.csv")

    top_100_honda = rdd_relevancy_honda.takeOrdered(100, key=(lambda x: -x[1]))
    result_honda = pd.DataFrame(top_100_honda, columns=(['word', 'relevancy']))
    result_honda.to_csv(f"s3://megadados-alunos/caio-pedro/tables/honda.csv")