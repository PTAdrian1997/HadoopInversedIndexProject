# HadoopInversedIndexProject
A Hadoop MapReduce project that computes the inversed index for a collection of books

An inversed index is a mapping from a unique word to a collection of records that hold the following:
  - the name of the file where it appears;
  - the line number where it appears in the file;
  - the position within that line;
