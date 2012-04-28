This is a large scale knn project designed to test various approaches from the literature.

The methods implemented here include:

  - k-means trees.  A k-means clustering is used to divide the points to be searched into clusters.  At search time, several 
nearby clusters are searched.

  - LSH approximate dot products.  Vectors are reduced using random projections to produce single bit hashes.  32 of these 
hashes are grouped together to form a single integer.  The neat property of these 32-bit combined hashes is that the xor
of the hashes of two vectors can be used to approximate the dot product of the vectors.  Then we can search for points that
have near zero xor results and have nearly the same vector length as the query.  These candidate points can be compared for
exact distance for final ranking.  The point is that xor's are faster than floating point vector distance computations and
the vector length is cached in Mahout vectors.  That makes this pre-search really fast and massively decreases the number of 
actual distances we need to compute.  LSH hashes can be used inside the k-means trees to make their distance computations
faster.

  - projection search.  See below, but the idea is that we use projection onto a single vector to order all the points to 
search.  The key characteristic of a projection is that nearby points before the projection will be near after the projection.
Of course, so will some other points but with multiple projections, it is likely that we can find the near points with
less effort than scanning everything.  Combining with LSH tricks can speed this up even more.  This is used inside the k-means
clustering to make that fast and can be used in the k-means trees to help find nearby clusters.

  - random projections.  If we have very high dimensional data with dimension d, then projection into a space with 
dimension > log d will preserve distances pretty well.  This can reduce very high dimensional problems to moderate dimensional
problems that we can attack with the methods above.  This isn't implemented here, but it is pretty trivial using normal
Mahout capabilities.

To recreate the pdf paper on the streaming k-means clustering algorithm implemented here, use the following commands in the `docs/scaling-k-means` directory:

    $ /usr/texbin/pdflatex scaling-k-means.tex 
    $ /usr/texbin/bibtex scaling-k-means
    $ /usr/texbin/pdflatex scaling-k-means.tex 
    $ /usr/texbin/pdflatex scaling-k-means.tex 

You will need to install pdftex to do this. MacTex and TexShop provide nice capabilities
for dealing with latex files.  See http://pages.uoregon.edu/koch/texshop/installing.html
and http://pages.uoregon.edu/koch/texshop/obtaining.html

This will produce a file called `scaling-k-means.pdf` that describes the basic clustering algorithm used here.  The idea is 
to do a self-adapting single-pass clustering algorithm to get a rough clustering of the data with lots and lots of clusters.
The guarantee here is that the clusters that result will be a decent surrogate for the distribution of the original data.  These clusters 
can then themselves be clustered in memory to get a high quality clustering of the original data.  Since we 
don't really care directly about the output of the single-pass clustering, we have latitude in the single-pass algorithm that 
would not normally be available.

Internally, the single-pass streaming k-means uses an approximate search for the nearest centroid.  Without this, clustering
with a large number of clusters would be extremely expensive computationally.  There are a number of approximate nearest
neighbor algorithms in this package.  The one used in the k-means code right now uses multiple projections to find potential
nearest neighbors.  The cool thing about each of these projections is that they give an ordering to the points being searched
and that ordering is such that nearby points will be nearby in the ordered projection.  There may be other points as well
that are accidentally nearby in the projection, but if we use several projections, we are pretty likely to find a point that
really is quite close.  The trick in the code is that the projected vectors are put into a TreeSet using the projected value
to order the points.  That makes the code very simple.

All of this is dependendent on the relatively new ability to wrap Mahout vectors efficiently.  This is provided by the
abstract class DelegatingVector.  The WeightedVector that is used by the projection search is an example of a DelegatingVector.
The Centroid class used in the clustering code is an extension of a WeightedVector.  That allows the clustering code to 
cluster Centroids as well as Vectors.


More details anon
