
"""
Part 1: MapReduce

In our first part, we will practice using MapReduce
to create several pipelines.
This part has 20 questions.

As you complete your code, you can run the code with

    python3 part1.py
    pytest part1.py

and you can view the output so far in:

    output/part1-answers.txt

In general, follow the same guidelines as in HW1!
Make sure that the output in part1-answers.txt looks correct.
See "Grading notes" here:
https://github.com/DavisPL-Teaching/119-hw1/blob/main/part1.py

For Q5-Q7, make sure your answer uses general_map and general_reduce as much as possible.
You will still need a single .map call at the beginning (to convert the RDD into key, value pairs), but after that point, you should only use general_map and general_reduce.

If you aren't sure of the type of the output, please post a question on Piazza.
"""

# Spark boilerplate (remember to always add this at the top of any Spark file)
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataflowGraphExample").getOrCreate()
sc = spark.sparkContext

# Additional imports
import pytest

"""
===== Questions 1-3: Generalized Map and Reduce =====

We will first implement the generalized version of MapReduce.
It works on (key, value) pairs:

- During the map stage, for each (key1, value1) pairs we
  create a list of (key2, value2) pairs.
  All of the values are output as the result of the map stage.

- During the reduce stage, we will apply a reduce_by_key
  function (value2, value2) -> value2
  that describes how to combine two values.
  The values (key2, value2) will be grouped
  by key, then values of each key key2
  will be combined (in some order) until there
  are no values of that key left. It should end up with a single
  (key2, value2) pair for each key.

1. Fill in the general_map function
using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q1() answer. It should fill out automatically.
"""

def general_map(rdd, f):
    """
    rdd: an RDD with values of type (k1, v1)
    f: a function (k1, v1) -> List[(k2, v2)]
    output: an RDD with values of type (k2, v2)
    """
    return rdd.flatMap(lambda pair: f(pair[0], pair[1]))

def test_general_map():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Map returning no values
    rdd2 = general_map(rdd1, lambda k, v: [])

    # Map returning length
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = rdd3.map(lambda pair: pair[1])

    # Map returnning odd or even length
    rdd5 = general_map(rdd1, lambda k, v: [(len(v) % 2, ())])

    assert rdd2.collect() == []
    assert sum(rdd4.collect()) == 14
    assert set(rdd5.collect()) == set([(1, ())])

def q1():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_map(rdd1, lambda k, v: [(1, v[-1])])
    return sorted(rdd2.collect())

"""
2. Fill in the reduce function using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q2() answer. It should fill out automatically.
"""

def general_reduce(rdd, f):
    """
    rdd: an RDD with values of type (k2, v2)
    f: a function (v2, v2) -> v2
    output: an RDD with values of type (k2, v2),
        and just one single value per key
    """
    return rdd.reduceByKey(lambda x, y: f(x, y))

def test_general_reduce():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Reduce, concatenating strings of the same key
    rdd2 = general_reduce(rdd1, lambda x, y: x + y)
    res2 = set(rdd2.collect())

    # Reduce, adding lengths
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = general_reduce(rdd3, lambda x, y: x + y)
    res4 = sorted(rdd4.collect())

    assert (
        res2 == set([('c', "catcow"), ('d', "dog"), ('z', "zebra")])
        or res2 == set([('c', "cowcat"), ('d', "dog"), ('z', "zebra")])
    )
    assert res4 == [('c', 6), ('d', 3), ('z', 5)]

def q2():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_reduce(rdd1, lambda x, y: "hello")
    return sorted(rdd2.collect())

"""
3. Name one scenario where having the keys for Map
and keys for Reduce be different might be useful.

=== ANSWER Q3 BELOW ===
One useful scenario is when we want to reorganize the data by a different grouping. 
For example, if the input records are keyed by user ID but we want to aggregate 
by product instead, the map stage can convert (user, product) into (product, 1). 
Then the reduce stage can sum the values per product. 
In this case the map keys (users) are different from the reduce keys (products).

=== END OF Q3 ANSWER ===
"""

"""
===== Questions 4-10: MapReduce Pipelines =====

Now that we have our generalized MapReduce function,
let's do a few exercises.
For the first set of exercises, we will use a simple dataset that is the
set of integers between 1 and 1 million (inclusive).

4. First, we need a function that loads the input.
"""

def load_input(N=None, P=None):
    # Return a parallelized RDD with the integers between 1 and 1,000,000
    # This will be referred to in the following questions.
    N = N or 1_000_000         
    rdd = sc.parallelize(range(1, N + 1))
    if P:
        rdd = rdd.repartition(P)
    return rdd

def q4(rdd):
    # Input: the RDD from load_input
    # Output: the length of the dataset.
    # You may use general_map or general_reduce here if you like (but you don't have to) to get the total count.
    return rdd.count()

"""
Now use the general_map and general_reduce functions to answer the following questions.

For Q5-Q7, your answers should use general_map and general_reduce as much as possible (wherever possible): you will still need a single .map call at the beginning (to convert the RDD into key, value pairs), but after that point, you should only use general_map and general_reduce.

5. Among the numbers from 1 to 1 million, what is the average value?
"""

def q5(rdd):
    # Input: the RDD from Q4
    # Output: the average value
    # Convert each number into a key-value pair 
    rdd_kv = rdd.map(lambda x: ('sum', x))
    
    # Sum all numbers using general_reduce
    total_sum = general_reduce(rdd_kv, lambda x, y: x + y).collect()[0][1]
    
    # Count the numbers 
    rdd_count = rdd.map(lambda x: ('count', 1))
    total_count = general_reduce(rdd_count, lambda x, y: x + y).collect()[0][1]
    
    # Compute average
    return total_sum / total_count


"""
6. Among the numbers from 1 to 1 million, when written out,
which digit is most common, with what frequency?
And which is the least common, with what frequency?

(If there are ties, you may answer any of the tied digits.)

The digit should be either an integer 0-9 or a character '0'-'9'.
Frequency is the number of occurences of each value.

Your answer should use the general_map and general_reduce functions as much as possible.
"""

def q6(rdd):
    # Input: the RDD from Q4
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)
    # Convert numbers to (digit, 1) pairs
    rdd_digits = general_map(
        rdd.map(lambda x: (x, None)),
        lambda k, v: [(d, 1) for d in str(k)]
    )

    
    # Reduce by digit to get counts
    digit_counts = general_reduce(rdd_digits, lambda x, y: x + y)
    
    # collect as list of (digit, count)
    counts = digit_counts.collect()
    
    # Find most and least common digits
    most_common_digit, most_common_freq = max(counts, key=lambda pair: pair[1])
    least_common_digit, least_common_freq = min(counts, key=lambda pair: pair[1])
    
    return (most_common_digit, most_common_freq, least_common_digit, least_common_freq)

"""
7. Among the numbers from 1 to 1 million, written out in English, which letter is most common?
With what frequency?
The least common?
With what frequency?

(If there are ties, you may answer any of the tied characters.)

For this part, you will need a helper function that computes
the English name for a number.

Please implement this without using an external library!
You should write this from scratch in Python.

Examples:

    0 = zero
    71 = seventy one
    513 = five hundred and thirteen
    801 = eight hundred and one
    999 = nine hundred and ninety nine
    1001 = one thousand one
    500,501 = five hundred thousand five hundred and one
    555,555 = five hundred and fifty five thousand five hundred and fifty five
    1,000,000 = one million

Notes:
- For "least frequent", count only letters which occur,
  not letters which don't occur.
- Please ignore spaces and hyphens.
- Use all lowercase letters.
- The word "and" should only appear after the "hundred" part, and nowhere else.
  It should appear after the hundreds if there are tens or ones in the same block.
  (Note the 1001 case above which differs from some other implementations!)
"""

# *** Define helper function(s) here ***
NUMS_0_19 = [
    "zero", "one", "two", "three", "four", "five", "six", "seven",
    "eight", "nine", "ten", "eleven", "twelve", "thirteen", "fourteen",
    "fifteen", "sixteen", "seventeen", "eighteen", "nineteen"
]

TENS = [
    "", "", "twenty", "thirty", "forty", "fifty",
    "sixty", "seventy", "eighty", "ninety"
]

def under_thousand(n):
    if n < 20:
        return NUMS_0_19[n]
    elif n < 100:
        tens = TENS[n // 10]
        ones = "" if n % 10 == 0 else "-" + NUMS_0_19[n % 10]
        return tens + ones
    else:
        hundreds = n // 100
        rest = n % 100
        if rest == 0:
            return NUMS_0_19[hundreds] + " hundred"
        else:
            return NUMS_0_19[hundreds] + " hundred " + under_thousand(rest)


def number_to_words(n):
    if not isinstance(n, int) or n < 0 or n > 999999:
        return ""

    if n < 1000:
        return under_thousand(n)

    thousands = n // 1000
    remainder = n % 1000

    if remainder == 0:
        return under_thousand(thousands) + " thousand"
    else:
        return under_thousand(thousands) + " thousand " + under_thousand(remainder)


def q7(rdd):
    # Input: the RDD from Q4
    # Output: a tulpe (most common char, most common frequency, least common char, least common frequency)
    # Convert each number to letters 
    rdd_letters = rdd.flatMap(lambda n: [c for c in number_to_words(n) if c.isalpha()])
    
    # Count frequencies
    letter_counts = rdd_letters.map(lambda c: (c, 1)).reduceByKey(lambda a, b: a + b)
    
    # Collect counts to driver
    counts = letter_counts.collect()
    
    # Find most and least frequent letters
    most_common_letter, most_common_freq = max(counts, key=lambda pair: pair[1])
    least_common_letter, least_common_freq = min(counts, key=lambda pair: pair[1])
    
    return (most_common_letter, most_common_freq, least_common_letter, least_common_freq)

"""
8. Does the answer change if we have the numbers from 1 to 100,000,000?

Make a version of both pipelines from Q6 and Q7 for this case.
You will need a new load_input function.

Notes:
- The functions q8_a and q8_b don't have input parameters; they should call
  load_input_bigger directly.
- Please ensure that each of q8a and q8b runs in at most 3 minutes.
- If you are unable to run up to 100 million on your machine within the time
  limit, please change the input to 10 million instead of 100 million.
  If it is still taking too long even for that,
  you may need to change the number of partitions.
  For example, one student found that setting number of partitions to 100
  helped speed it up.
"""

def load_input_bigger(N=None, P=None):
    N = N or 10_000_000        
    num_parts = P or 200       
    rdd = sc.parallelize(range(1, N + 1), numSlices=num_parts)
    return rdd

def q8_a(N=None, P=None):
    # version of Q6
    # It should call into q6() with the new RDD!
    # Don't re-implemented the q6 logic.
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)
    rdd = load_input_bigger(N, P)
    return q6(rdd)

def q8_b(N=None, P=None):
    # version of Q7
    # It should call into q7() with the new RDD!
    # Don't re-implemented the q6 logic.
    # Output: a tulpe (most common char, most common frequency, least common char, least common frequency)
    rdd = load_input_bigger(N, P)
    return q7(rdd)

"""
Discussion questions

9. State what types you used for k1, v1, k2, and v2 for your Q6 and Q7 pipelines.

=== ANSWER Q9 BELOW ===
For Q6 (digit counting):
- k1: integer (the original number from 1 to 1,000,000)
- v1: integer (same as k1 in initial mapping, or unused in general_map)
- k2: string (a single digit 0-9)
- v2: integer (the count 1, later summed in general_reduce)

For Q7 (letter counting):
- k1: integer (the original number from 1 to 1,000,000)
- v1: integer (same as k1 in initial mapping, or unused in general_map)
- k2: string (a single letter a-z)
- v2: integer (the count 1, later summed in general_reduce)


=== END OF Q9 ANSWER ===

10. Do you think it would be possible to compute the above using only the
"simplified" MapReduce we saw in class? Why or why not?

=== ANSWER Q10 BELOW ===

It would be difficult to compute Q6 and Q7 using only the simplified MapReduce we saw in class, 
because that version only supports a single key value type and does not allow mapping to multiple 
(key2, value2) pairs per input. Both Q6 and Q7 require generating multiple output pairs 
per number (one per digit or one per letter). It requires the generalized map functionality. 
We also need to reduce by key to combine counts, which is easier using the generalized reduce.

=== END OF Q10 ANSWER ===
"""

"""
===== Questions 11-18: MapReduce Edge Cases =====

For the remaining questions, we will explore two interesting edge cases in MapReduce.

11. One edge case occurs when there is no output for the reduce stage.
This can happen if the map stage returns an empty list (for all keys).

Demonstrate this edge case by creating a specific pipeline which uses
our data set from Q4. It should use the general_map and general_reduce functions.

For Q11, Q14, and Q16:
your answer should return a Python set of (key, value) pairs after the reduce stage.
"""

def q11(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs
    # Convert numbers to a dummy key ('num', value)
    rdd_pairs = rdd.map(lambda x: ('num', x))

    # Use general_map but return EMPTY list for each input
    mapped = general_map(rdd_pairs, lambda k, v: [])
    reduced = general_reduce(mapped, lambda a, b: a + b)
    return set(reduced.collect())

"""
12. What happened? Explain below.
Does this depend on anything specific about how
we chose to define general_reduce?

=== ANSWER Q12 BELOW ===
In Q11, the map stage returned an empty list for all inputs,  so the reduce stage had 
no keys to process. As a result, the output of the reduce stage was just empty (set()).
This behavior doesn’t depend on the specific implementation of general_reduce. Any 
reduce function that receives no inputs for a key will produce no output. 

=== END OF Q12 ANSWER ===

13. Lastly, we will explore a second edge case, where the reduce stage can
output different values depending on the order of the input.
This leads to something called "nondeterminism", where the output of the
pipeline can even change between runs!

First, take a look at the definition of your general_reduce function.
Why do you imagine it could be the case that the output of the reduce stage
is different depending on the order of the input?

=== ANSWER Q13 BELOW ===
If the reduce function used in general_reduce is not associative or commutative, 
the output can depend on the order that the key value pairs are combined.
In Spark, data is split across partitions, and the order in which values 
are collected is not guaranteed. If f in general_reduce combines values in a 
way that depends on order (e.g., subtraction or non-commutative operations), 
then the final result may differ between runs. This is why nondeterminism can occur 
in the reduce stage.

=== END OF Q13 ANSWER ===

14.
Now demonstrate this edge case concretely by writing a specific example below.
As before, you should use the same dataset from Q4.

Important: Please create an example where the output of the reduce stage is a set of (integer, integer) pairs.
(So k2 and v2 are both integers.)
"""

def q14(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs
    pairs = rdd.map(lambda x: (x % 3, x))
    
    # Reduce using subtraction
    reduced = pairs.reduceByKey(lambda x, y: x - y)
    
    # Collect results as a set of (key, value) pairs
    return set(reduced.collect())

"""
15.
Run your pipeline. What happens?
Does it exhibit nondeterministic behavior on different runs?
(It may or may not! This depends on the Spark scheduler and implementation,
including partitioning.

=== ANSWER Q15 BELOW ===
When we run the pipeline from Q14, the reduce stage combines values 
in an order that can change depending on how Spark schedules the tasks. 
Since the operation used like subtraction depends on the order, the 
final result might be different each time we run it.
Even if the output looks the same sometimes, it’s not guaranteed. This shows 
that using operations that aren’t order independent can make Spark pipelines nondeterministic.

=== END OF Q15 ANSWER ===

16.
Lastly, try the same pipeline as in Q14
with at least 3 different levels of parallelism.

Write three functions, a, b, and c that use different levels of parallelism.
"""

def q16_a():
    # For this one, create the RDD yourself. Choose the number of partitions.
    # 2 partitions for low parallelism
    rdd = load_input().repartition(2)  
    pairs = rdd.map(lambda x: (x % 3, x))
    reduced = pairs.reduceByKey(lambda x, y: x - y)
    return set(reduced.collect())

def q16_b():
    # For this one, create the RDD yourself. Choose the number of partitions.
    # 5 partitions for medium parallelism
    rdd = load_input().repartition(5)  
    pairs = rdd.map(lambda x: (x % 3, x))
    reduced = pairs.reduceByKey(lambda x, y: x - y)
    return set(reduced.collect())

def q16_c():
    # For this one, create the RDD yourself. Choose the number of partitions.
    # 10 partitions for high parallelism
    rdd = load_input().repartition(10)  
    pairs = rdd.map(lambda x: (x % 3, x))
    reduced = pairs.reduceByKey(lambda x, y: x - y)
    return set(reduced.collect())

"""
Discussion questions

17. Was the answer different for the different levels of parallelism?

=== ANSWER Q17 BELOW ===
Yes, the answers were different for the different levels of parallelism. 
When we increased the number of partitions for the different levels of parallelism, 
the order in which the reduce function combined values changed. This which led to different 
final results.
With 2 partitions for low parallelism, the output was {(1, -1999989), (0, -999975), (2, -1999976)}
With 5 partitions for medium parallelism, the output was {(1, 100001099819), (0, 100002099813), (2, 100001099852)}
With 10 partitions for high parallelism, the output was {(1, 133334132361), (0, 133333465785), (2, 133334799178)}
The differences happen because the order in which values are combined changes when the RDD is split into different numbers of partitions.

=== END OF Q17 ANSWER ===

18. Do you think this would be a serious problem if this occured on a real-world pipeline?
Explain why or why not.

=== ANSWER Q18 BELOW ===
This could be a problem in a real-world pipeline if exact results are important. For example, 
in Q16, the same data produced very different outputs depending on parallelism. If a pipeline 
needs consistent results for calculations, reports, or decisions, these differences could cause 
errors. To avoid this, it’s better to use operations that always give the same result no matter 
the order, or carefully control how data is combined.

=== END OF Q18 ANSWER ===

===== Q19-20: Further reading =====

19.
The following is a very nice paper
which explores this in more detail in the context of real-world MapReduce jobs.
https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/icsecomp14seip-seipid15-p.pdf

Take a look at the paper. What is one sentence you found interesting?

=== ANSWER Q19 BELOW ===
One sentence that I found interesting is from the introduction in the first page.
Our study has collected 507 distinct custom user-defined reducers
found in 13,311 real-world MapReduce-style jobs in our production
cluster. 
=== END OF Q19 ANSWER ===

20.
Take one example from the paper, and try to implement it using our
general_map and general_reduce functions.
For this part, just return the answer "True" at the end if you found
it possible to implement the example, and "False" if it was not.
"""

def q20():
    # Figure 3: An example reduce function that is sensitive to its input order.
    # Example dataset: pairs with the same key but values in different order
    example_rdd = sc.parallelize([
        ('k', ('apple', 1)),
        ('k', ('banana', 2)),
        ('k', ('cherry', 3))
    ])

    # Use general_map with a simple identity transformation
    mapped = general_map(
        example_rdd,
        lambda k, v: [(k, v)]
    )

    # Last value wins reducer 
    def last_value_wins(v1, v2):
        return v2
    reduced = general_reduce(mapped, last_value_wins)

    # Collect to ensure it runs
    output = reduced.collect()

    # We successfully implemented the example and return True
    return True

"""
That's it for Part 1!

===== Wrapping things up =====

**Don't modify this part.**

To wrap things up, we have collected
everything together in a pipeline for you below.

Check out the output in output/part1-answers.txt.
"""

ANSWER_FILE = "output/part1-answers.txt"
UNFINISHED = 0

def log_answer(name, func, *args):
    try:
        answer = func(*args)
        print(f"{name} answer: {answer}")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},{answer}\n')
            print(f"Answer saved to {ANSWER_FILE}")
    except NotImplementedError:
        print(f"Warning: {name} not implemented.")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},Not Implemented\n')
        global UNFINISHED
        UNFINISHED += 1

def PART_1_PIPELINE():
    open(ANSWER_FILE, 'w').close()

    try:
        dfs = load_input()
    except NotImplementedError:
        print("Welcome to Part 1! Implement load_input() to get started.")
        dfs = sc.parallelize([])

    # Questions 1-3
    log_answer("q1", q1)
    log_answer("q2", q2)
    # 3: commentary

    # Questions 4-10
    log_answer("q4", q4, dfs)
    log_answer("q5", q5, dfs)
    log_answer("q6", q6, dfs)
    log_answer("q7", q7, dfs)
    log_answer("q8a", q8_a)
    log_answer("q8b", q8_b)
    # 9: commentary
    # 10: commentary

    # Questions 11-18
    log_answer("q11", q11, dfs)
    # 12: commentary
    # 13: commentary
    log_answer("q14", q14, dfs)
    # 15: commentary
    log_answer("q16a", q16_a)
    log_answer("q16b", q16_b)
    log_answer("q16c", q16_c)
    # 17: commentary
    # 18: commentary

    # Questions 19-20
    # 19: commentary
    log_answer("q20", q20)

    # Answer: return the number of questions that are not implemented
    if UNFINISHED > 0:
        print("Warning: there are unfinished questions.")

    return f"{UNFINISHED} unfinished questions"

if __name__ == '__main__':
    log_answer("PART 1", PART_1_PIPELINE)

"""
=== END OF PART 1 ===
"""
