#!/usr/bin/python
from __future__ import division, print_function, absolute_import

from functools import reduce
import numpy as np
from scipy import special
import math
#from scipy.lib.six import callable, string_types
string_types = basestring
def margins(a):
    """Return a list of the marginal sums of the array `a`.

    Parameters
    ----------
    a : ndarray
        The array for which to compute the marginal sums.

    Returns
    -------
    margsums : list of ndarrays
        A list of length `a.ndim`.  `margsums[k]` is the result
        of summing `a` over all axes except `k`; it has the same
        number of dimensions as `a`, but the length of each axis
        except axis `k` will be 1.

    Examples
    --------
    >>> a = np.arange(12).reshape(2, 6)
    >>> a
    array([[ 0,  1,  2,  3,  4,  5],
           [ 6,  7,  8,  9, 10, 11]])
    >>> m0, m1 = margins(a)
    >>> m0
    array([[15],
           [51]])
    >>> m1
    array([[ 6,  8, 10, 12, 14, 16]])

    >>> b = np.arange(24).reshape(2,3,4)
    >>> m0, m1, m2 = margins(b)
    >>> m0
    array([[[ 66]],
           [[210]]])
    >>> m1
    array([[[ 60],
            [ 92],
            [124]]])
    >>> m2
    array([[[60, 66, 72, 78]]])
    """
    margsums = []
    ranged = list(range(a.ndim))
    for k in ranged:
        marg = np.apply_over_axes(np.sum, a, [j for j in ranged if j != k])
        margsums.append(marg)
    return margsums


def expected_freq(observed):
    """
    Compute the expected frequencies from a contingency table.

    Given an n-dimensional contingency table of observed frequencies,
    compute the expected frequencies for the table based on the marginal
    sums under the assumption that the groups associated with each
    dimension are independent.

    Parameters
    ----------
    observed : array_like
        The table of observed frequencies.  (While this function can handle
        a 1-D array, that case is trivial.  Generally `observed` is at
        least 2-D.)

    Returns
    -------
    expected : ndarray of float64
        The expected frequencies, based on the marginal sums of the table.
        Same shape as `observed`.

    Examples
    --------
    >>> observed = np.array([[10, 10, 20],[20, 20, 20]])
    >>> expected_freq(observed)
    array([[ 12.,  12.,  16.],
           [ 18.,  18.,  24.]])

    """
    # Typically `observed` is an integer array. If `observed` has a large
    # number of dimensions or holds large values, some of the following
    # computations may overflow, so we first switch to floating point.
    observed = np.asarray(observed, dtype=np.float64)

    # Create a list of the marginal sums.
    margsums = margins(observed)

    # Create the array of expected frequencies.  The shapes of the
    # marginal sums returned by apply_over_axes() are just what we
    # need for broadcasting in the following product.
    d = observed.ndim
    expected = reduce(np.multiply, margsums) / observed.sum() ** (d - 1)
    return expected


def chi2_contingency(observed, correction=True, lambda_=None):
    """Chi-square test of independence of variables in a contingency table.

    This function computes the chi-square statistic and p-value for the
    hypothesis test of independence of the observed frequencies in the
    contingency table [1]_ `observed`.  The expected frequencies are computed
    based on the marginal sums under the assumption of independence; see
    `scipy.stats.contingency.expected_freq`.  The number of degrees of
    freedom is (expressed using numpy functions and attributes)::

        dof = observed.size - sum(observed.shape) + observed.ndim - 1


    Parameters
    ----------
    observed : array_like
        The contingency table. The table contains the observed frequencies
        (i.e. number of occurrences) in each category.  In the two-dimensional
        case, the table is often described as an "R x C table".
    correction : bool, optional
        If True, *and* the degrees of freedom is 1, apply Yates' correction
        for continuity.  The effect of the correction is to adjust each
        observed value by 0.5 towards the corresponding expected value.
    lambda_ : float or str, optional.
        By default, the statistic computed in this test is Pearson's
        chi-squared statistic [2]_.  `lambda_` allows a statistic from the
        Cressie-Read power divergence family [3]_ to be used instead.  See
        `power_divergence` for details.

    Returns
    -------
    chi2 : float
        The test statistic.
    p : float
        The p-value of the test
    dof : int
        Degrees of freedom
    expected : ndarray, same shape as `observed`
        The expected frequencies, based on the marginal sums of the table.

    See Also
    --------
    contingency.expected_freq
    fisher_exact
    chisquare
    power_divergence

    Notes
    -----
    An often quoted guideline for the validity of this calculation is that
    the test should be used only if the observed and expected frequency in
    each cell is at least 5.

    This is a test for the independence of different categories of a
    population. The test is only meaningful when the dimension of
    `observed` is two or more.  Applying the test to a one-dimensional
    table will always result in `expected` equal to `observed` and a
    chi-square statistic equal to 0.

    This function does not handle masked arrays, because the calculation
    does not make sense with missing values.

    Like stats.chisquare, this function computes a chi-square statistic;
    the convenience this function provides is to figure out the expected
    frequencies and degrees of freedom from the given contingency table.
    If these were already known, and if the Yates' correction was not
    required, one could use stats.chisquare.  That is, if one calls::

        chi2, p, dof, ex = chi2_contingency(obs, correction=False)

    then the following is true::

        (chi2, p) == stats.chisquare(obs.ravel(), f_exp=ex.ravel(),
                                     ddof=obs.size - 1 - dof)

    The `lambda_` argument was added in version 0.13.0 of scipy.

    References
    ----------
    .. [1] "Contingency table", http://en.wikipedia.org/wiki/Contingency_table
    .. [2] "Pearson's chi-squared test",
           http://en.wikipedia.org/wiki/Pearson%27s_chi-squared_test
    .. [3] Cressie, N. and Read, T. R. C., "Multinomial Goodness-of-Fit
           Tests", J. Royal Stat. Soc. Series B, Vol. 46, No. 3 (1984),
           pp. 440-464.

    Examples
    --------
    A two-way example (2 x 3):

    >>> obs = np.array([[10, 10, 20], [20, 20, 20]])
    >>> chi2_contingency(obs)
    (2.7777777777777777,
     0.24935220877729619,
     2,
     array([[ 12.,  12.,  16.],
            [ 18.,  18.,  24.]]))

    Perform the test using the log-likelihood ratio (i.e. the "G-test")
    instead of Pearson's chi-squared statistic.

    >>> g, p, dof, expctd = chi2_contingency(obs, lambda_="log-likelihood")
    >>> g, p
    (2.7688587616781319, 0.25046668010954165)

    A four-way example (2 x 2 x 2 x 2):

    >>> obs = np.array(
    ...     [[[[12, 17],
    ...        [11, 16]],
    ...       [[11, 12],
    ...        [15, 16]]],
    ...      [[[23, 15],
    ...        [30, 22]],
    ...       [[14, 17],
    ...        [15, 16]]]])
    >>> chi2_contingency(obs)
    (8.7584514426741897,
     0.64417725029295503,
     11,
     array([[[[ 14.15462386,  14.15462386],
              [ 16.49423111,  16.49423111]],
             [[ 11.2461395 ,  11.2461395 ],
              [ 13.10500554,  13.10500554]]],
            [[[ 19.5591166 ,  19.5591166 ],
              [ 22.79202844,  22.79202844]],
             [[ 15.54012004,  15.54012004],
              [ 18.10873492,  18.10873492]]]]))
    """
    observed = np.asarray(observed)
    if np.any(observed < 0):
        raise ValueError("All values in `observed` must be nonnegative.")
    if observed.size == 0:
        raise ValueError("No data; `observed` has size 0.")

    expected = expected_freq(observed)
    if np.any(expected == 0):
        # Include one of the positions where expected is zero in
        # the exception message.
        zeropos = list(np.where(expected == 0)[0])
        raise ValueError("The internally computed table of expected "
                         "frequencies has a zero element at %s." % zeropos)

    # The degrees of freedom
    dof = expected.size - sum(expected.shape) + expected.ndim - 1

    if dof == 0:
        # Degenerate case; this occurs when `observed` is 1D (or, more
        # generally, when it has only one nontrivial dimension).  In this
        # case, we also have observed == expected, so chi2 is 0.
        chi2 = 0.0
        p = 1.0
    else:
        if dof == 1 and correction:
            # Adjust `observed` according to Yates' correction for continuity.
            observed = observed + 0.5 * np.sign(expected - observed)

        chi2, p = power_divergence(observed, expected,
                                   ddof=observed.size - 1 - dof, axis=None,
                                   lambda_=lambda_)

    return chi2, p, dof, expected


# Map from names to lambda_ values used in power_divergence().
_power_div_lambda_names = {
    "pearson": 1,
    "log-likelihood": 0,
    "freeman-tukey": -0.5,
    "mod-log-likelihood": -1,
    "neyman": -2,
    "cressie-read": 2/3,
}


def _count(a, axis=None):
    """
    Count the number of non-masked elements of an array.

    This function behaves like np.ma.count(), but is much faster
    for ndarrays.
    """
    if hasattr(a, 'count'):
        num = a.count(axis=axis)
        if isinstance(num, np.ndarray) and num.ndim == 0:
            # In some cases, the `count` method returns a scalar array (e.g.
            # np.array(3)), but we want a plain integer.
            num = int(num)
    else:
        if axis is None:
            num = a.size
        else:
            num = a.shape[axis]
    return num


def power_divergence(f_obs, f_exp=None, ddof=0, axis=0, lambda_=None):
    """
    Cressie-Read power divergence statistic and goodness of fit test.

    This function tests the null hypothesis that the categorical data
    has the given frequencies, using the Cressie-Read power divergence
    statistic.

    Parameters
    ----------
    f_obs : array
        Observed frequencies in each category.
    f_exp : array, optional
        Expected frequencies in each category.  By default the categories are
        assumed to be equally likely.
    ddof : int, optional
        "Delta degrees of freedom": adjustment to the degrees of freedom
        for the p-value.  The p-value is computed using a chi-squared
        distribution with ``k - 1 - ddof`` degrees of freedom, where `k`
        is the number of observed frequencies.  The default value of `ddof`
        is 0.
    axis : int or None, optional
        The axis of the broadcast result of `f_obs` and `f_exp` along which to
        apply the test.  If axis is None, all values in `f_obs` are treated
        as a single data set.  Default is 0.
    lambda_ : float or str, optional
        `lambda_` gives the power in the Cressie-Read power divergence
        statistic.  The default is 1.  For convenience, `lambda_` may be
        assigned one of the following strings, in which case the
        corresponding numerical value is used::

            String              Value   Description
            "pearson"             1     Pearson's chi-squared statistic.
                                        In this case, the function is
                                        equivalent to `stats.chisquare`.
            "log-likelihood"      0     Log-likelihood ratio. Also known as
                                        the G-test [3]_.
            "freeman-tukey"      -1/2   Freeman-Tukey statistic.
            "mod-log-likelihood" -1     Modified log-likelihood ratio.
            "neyman"             -2     Neyman's statistic.
            "cressie-read"        2/3   The power recommended in [5]_.

    Returns
    -------
    stat : float or ndarray
        The Cressie-Read power divergence test statistic.  The value is
        a float if `axis` is None or if` `f_obs` and `f_exp` are 1-D.
    p : float or ndarray
        The p-value of the test.  The value is a float if `ddof` and the
        return value `stat` are scalars.

    See Also
    --------
    chisquare

    Notes
    -----
    This test is invalid when the observed or expected frequencies in each
    category are too small.  A typical rule is that all of the observed
    and expected frequencies should be at least 5.

    When `lambda_` is less than zero, the formula for the statistic involves
    dividing by `f_obs`, so a warning or error may be generated if any value
    in `f_obs` is 0.

    Similarly, a warning or error may be generated if any value in `f_exp` is
    zero when `lambda_` >= 0.

    The default degrees of freedom, k-1, are for the case when no parameters
    of the distribution are estimated. If p parameters are estimated by
    efficient maximum likelihood then the correct degrees of freedom are
    k-1-p. If the parameters are estimated in a different way, then the
    dof can be between k-1-p and k-1. However, it is also possible that
    the asymptotic distribution is not a chisquare, in which case this
    test is not appropriate.

    This function handles masked arrays.  If an element of `f_obs` or `f_exp`
    is masked, then data at that position is ignored, and does not count
    towards the size of the data set.

    .. versionadded:: 0.13.0

    References
    ----------
    .. [1] Lowry, Richard.  "Concepts and Applications of Inferential
           Statistics". Chapter 8. http://faculty.vassar.edu/lowry/ch8pt1.html
    .. [2] "Chi-squared test", http://en.wikipedia.org/wiki/Chi-squared_test
    .. [3] "G-test", http://en.wikipedia.org/wiki/G-test
    .. [4] Sokal, R. R. and Rohlf, F. J. "Biometry: the principles and
           practice of statistics in biological research", New York: Freeman
           (1981)
    .. [5] Cressie, N. and Read, T. R. C., "Multinomial Goodness-of-Fit
           Tests", J. Royal Stat. Soc. Series B, Vol. 46, No. 3 (1984),
           pp. 440-464.

    Examples
    --------

    (See `chisquare` for more examples.)

    When just `f_obs` is given, it is assumed that the expected frequencies
    are uniform and given by the mean of the observed frequencies.  Here we
    perform a G-test (i.e. use the log-likelihood ratio statistic):

    >>> power_divergence([16, 18, 16, 14, 12, 12], method='log-likelihood')
    (2.006573162632538, 0.84823476779463769)

    The expected frequencies can be given with the `f_exp` argument:

    >>> power_divergence([16, 18, 16, 14, 12, 12],
    ...                  f_exp=[16, 16, 16, 16, 16, 8],
    ...                  lambda_='log-likelihood')
    (3.5, 0.62338762774958223)

    When `f_obs` is 2-D, by default the test is applied to each column.

    >>> obs = np.array([[16, 18, 16, 14, 12, 12], [32, 24, 16, 28, 20, 24]]).T
    >>> obs.shape
    (6, 2)
    >>> power_divergence(obs, lambda_="log-likelihood")
    (array([ 2.00657316,  6.77634498]), array([ 0.84823477,  0.23781225]))

    By setting ``axis=None``, the test is applied to all data in the array,
    which is equivalent to applying the test to the flattened array.

    >>> power_divergence(obs, axis=None)
    (23.31034482758621, 0.015975692534127565)
    >>> power_divergence(obs.ravel())
    (23.31034482758621, 0.015975692534127565)

    `ddof` is the change to make to the default degrees of freedom.

    >>> power_divergence([16, 18, 16, 14, 12, 12], ddof=1)
    (2.0, 0.73575888234288467)

    The calculation of the p-values is done by broadcasting the
    test statistic with `ddof`.

    >>> power_divergence([16, 18, 16, 14, 12, 12], ddof=[0,1,2])
    (2.0, array([ 0.84914504,  0.73575888,  0.5724067 ]))

    `f_obs` and `f_exp` are also broadcast.  In the following, `f_obs` has
    shape (6,) and `f_exp` has shape (2, 6), so the result of broadcasting
    `f_obs` and `f_exp` has shape (2, 6).  To compute the desired chi-squared
    statistics, we must use ``axis=1``:

    >>> power_divergence([16, 18, 16, 14, 12, 12],
    ...                  f_exp=[[16, 16, 16, 16, 16, 8],
    ...                         [8, 20, 20, 16, 12, 12]],
    ...                  axis=1)
    (array([ 3.5 ,  9.25]), array([ 0.62338763,  0.09949846]))

    """
    # Convert the input argument `lambda_` to a numerical value.
    if isinstance(lambda_, string_types):
        if lambda_ not in _power_div_lambda_names:
            names = repr(list(_power_div_lambda_names.keys()))[1:-1]
            raise ValueError("invalid string for lambda_: {0!r}.  Valid strings "
                "are {1}".format(lambda_, names))
        lambda_ = _power_div_lambda_names[lambda_]
    elif lambda_ is None:
        lambda_ = 1

    f_obs = np.asanyarray(f_obs)

    if f_exp is not None:
        f_exp = np.atleast_1d(np.asanyarray(f_exp))
    else:
        # Compute the equivalent of
        #   f_exp = f_obs.mean(axis=axis, keepdims=True)
        # Older versions of numpy do not have the 'keepdims' argument, so
        # we have to do a little work to achieve the same result.
        # Ignore 'invalid' errors so the edge case of a data set with length 0
        # is handled without spurious warnings.
        with np.errstate(invalid='ignore'):
            f_exp = np.atleast_1d(f_obs.mean(axis=axis))
        if axis is not None:
            reduced_shape = list(f_obs.shape)
            reduced_shape[axis] = 1
            f_exp.shape = reduced_shape

    # `terms` is the array of terms that are summed along `axis` to create
    # the test statistic.  We use some specialized code for a few special
    # cases of lambda_.
    if lambda_ == 1:
        # Pearson's chi-squared statistic
        terms = (f_obs - f_exp)**2 / f_exp
    elif lambda_ == 0:
        # Log-likelihood ratio (i.e. G-test)
        terms = 2.0 * (f_obs* np.log(f_obs / f_exp))
    elif lambda_ == -1:
        # Modified log-likelihood ratio
        terms = 2.0 * (f_exp * np.log(f_exp / f_obs))
    else:
        # General Cressie-Read power divergence.
        terms = f_obs * ((f_obs / f_exp)**lambda_ - 1)
        terms /= 0.5 * lambda_ * (lambda_ + 1)

    stat = terms.sum(axis=axis)

    num_obs = _count(terms, axis=axis)
    ddof = np.asarray(ddof)
    p = chisqprob(stat, num_obs - 1 - ddof)

    return stat, p


def chisquare(f_obs, f_exp=None, ddof=0, axis=0):
    """
    Calculates a one-way chi square test.

    The chi square test tests the null hypothesis that the categorical data
    has the given frequencies.

    Parameters
    ----------
    f_obs : array
        Observed frequencies in each category.
    f_exp : array, optional
        Expected frequencies in each category.  By default the categories are
        assumed to be equally likely.
    ddof : int, optional
        "Delta degrees of freedom": adjustment to the degrees of freedom
        for the p-value.  The p-value is computed using a chi-squared
        distribution with ``k - 1 - ddof`` degrees of freedom, where `k`
        is the number of observed frequencies.  The default value of `ddof`
        is 0.
    axis : int or None, optional
        The axis of the broadcast result of `f_obs` and `f_exp` along which to
        apply the test.  If axis is None, all values in `f_obs` are treated
        as a single data set.  Default is 0.

    Returns
    -------
    chisq : float or ndarray
        The chi-squared test statistic.  The value is a float if `axis` is
        None or `f_obs` and `f_exp` are 1-D.
    p : float or ndarray
        The p-value of the test.  The value is a float if `ddof` and the
        return value `chisq` are scalars.

    See Also
    --------
    power_divergence
    mstats.chisquare

    Notes
    -----
    This test is invalid when the observed or expected frequencies in each
    category are too small.  A typical rule is that all of the observed
    and expected frequencies should be at least 5.

    The default degrees of freedom, k-1, are for the case when no parameters
    of the distribution are estimated. If p parameters are estimated by
    efficient maximum likelihood then the correct degrees of freedom are
    k-1-p. If the parameters are estimated in a different way, then the
    dof can be between k-1-p and k-1. However, it is also possible that
    the asymptotic distribution is not a chisquare, in which case this
    test is not appropriate.

    References
    ----------
    .. [1] Lowry, Richard.  "Concepts and Applications of Inferential
           Statistics". Chapter 8. http://faculty.vassar.edu/lowry/ch8pt1.html
    .. [2] "Chi-squared test", http://en.wikipedia.org/wiki/Chi-squared_test

    Examples
    --------
    When just `f_obs` is given, it is assumed that the expected frequencies
    are uniform and given by the mean of the observed frequencies.

    >>> chisquare([16, 18, 16, 14, 12, 12])
    (2.0, 0.84914503608460956)

    With `f_exp` the expected frequencies can be given.

    >>> chisquare([16, 18, 16, 14, 12, 12], f_exp=[16, 16, 16, 16, 16, 8])
    (3.5, 0.62338762774958223)

    When `f_obs` is 2-D, by default the test is applied to each column.

    >>> obs = np.array([[16, 18, 16, 14, 12, 12], [32, 24, 16, 28, 20, 24]]).T
    >>> obs.shape
    (6, 2)
    >>> chisquare(obs)
    (array([ 2.        ,  6.66666667]), array([ 0.84914504,  0.24663415]))

    By setting ``axis=None``, the test is applied to all data in the array,
    which is equivalent to applying the test to the flattened array.

    >>> chisquare(obs, axis=None)
    (23.31034482758621, 0.015975692534127565)
    >>> chisquare(obs.ravel())
    (23.31034482758621, 0.015975692534127565)

    `ddof` is the change to make to the default degrees of freedom.

    >>> chisquare([16, 18, 16, 14, 12, 12], ddof=1)
    (2.0, 0.73575888234288467)

    The calculation of the p-values is done by broadcasting the
    chi-squared statistic with `ddof`.

    >>> chisquare([16, 18, 16, 14, 12, 12], ddof=[0,1,2])
    (2.0, array([ 0.84914504,  0.73575888,  0.5724067 ]))

    `f_obs` and `f_exp` are also broadcast.  In the following, `f_obs` has
    shape (6,) and `f_exp` has shape (2, 6), so the result of broadcasting
    `f_obs` and `f_exp` has shape (2, 6).  To compute the desired chi-squared
    statistics, we use ``axis=1``:

    >>> chisquare([16, 18, 16, 14, 12, 12],
    ...           f_exp=[[16, 16, 16, 16, 16, 8], [8, 20, 20, 16, 12, 12]],
    ...           axis=1)
    (array([ 3.5 ,  9.25]), array([ 0.62338763,  0.09949846]))

    """
    return power_divergence(f_obs, f_exp=f_exp, ddof=ddof, axis=axis,
                            lambda_="pearson")


def ks_2samp(data1, data2):
    """
    Computes the Kolmogorov-Smirnov statistic on 2 samples.

    This is a two-sided test for the null hypothesis that 2 independent samples
    are drawn from the same continuous distribution.

    Parameters
    ----------
    a, b : sequence of 1-D ndarrays
        two arrays of sample observations assumed to be drawn from a continuous
        distribution, sample sizes can be different

    Returns
    -------
    D : float
        KS statistic
    p-value : float
        two-tailed p-value

    Notes
    -----
    This tests whether 2 samples are drawn from the same distribution. Note
    that, like in the case of the one-sample K-S test, the distribution is
    assumed to be continuous.

    This is the two-sided test, one-sided tests are not implemented.
    The test uses the two-sided asymptotic Kolmogorov-Smirnov distribution.

    If the K-S statistic is small or the p-value is high, then we cannot
    reject the hypothesis that the distributions of the two samples
    are the same.

    Examples
    --------
    >>> from scipy import stats
    >>> np.random.seed(12345678)  #fix random seed to get the same result
    >>> n1 = 200  # size of first sample
    >>> n2 = 300  # size of second sample

    For a different distribution, we can reject the null hypothesis since the
    pvalue is below 1%:

    >>> rvs1 = stats.norm.rvs(size=n1, loc=0., scale=1)
    >>> rvs2 = stats.norm.rvs(size=n2, loc=0.5, scale=1.5)
    >>> stats.ks_2samp(rvs1, rvs2)
    (0.20833333333333337, 4.6674975515806989e-005)

    For a slightly different distribution, we cannot reject the null hypothesis
    at a 10% or lower alpha since the p-value at 0.144 is higher than 10%

    >>> rvs3 = stats.norm.rvs(size=n2, loc=0.01, scale=1.0)
    >>> stats.ks_2samp(rvs1, rvs3)
    (0.10333333333333333, 0.14498781825751686)

    For an identical distribution, we cannot reject the null hypothesis since
    the p-value is high, 41%:

    >>> rvs4 = stats.norm.rvs(size=n2, loc=0.0, scale=1.0)
    >>> stats.ks_2samp(rvs1, rvs4)
    (0.07999999999999996, 0.41126949729859719)

    """
    data1, data2 = map(np.asarray, (data1, data2))
    n1 = data1.shape[0]
    n2 = data2.shape[0]
    n1 = len(data1)
    n2 = len(data2)
    data1 = np.sort(data1)
    data2 = np.sort(data2)
    data_all = np.concatenate([data1,data2])
    cdf1 = np.searchsorted(data1,data_all,side='right')/(1.0*n1)
    cdf2 = (np.searchsorted(data2,data_all,side='right'))/(1.0*n2)
    d = np.max(np.absolute(cdf1-cdf2))
    # Note: d absolute not signed distance
    en = np.sqrt(n1*n2/float(n1+n2))
    try:
        prob = ksprob((en+0.12+0.11/en)*d)
    except:
        prob = 1.0
    return d, prob


def mannwhitneyu(x, y, use_continuity=True):
    """
    Computes the Mann-Whitney rank test on samples x and y.

    Parameters
    ----------
    x, y : array_like
        Array of samples, should be one-dimensional.
    use_continuity : bool, optional
            Whether a continuity correction (1/2.) should be taken into
            account. Default is True.

    Returns
    -------
    u : float
        The Mann-Whitney statistics.
    prob : float
        One-sided p-value assuming a asymptotic normal distribution.

    Notes
    -----
    Use only when the number of observation in each sample is > 20 and
    you have 2 independent samples of ranks. Mann-Whitney U is
    significant if the u-obtained is LESS THAN or equal to the critical
    value of U.

    This test corrects for ties and by default uses a continuity correction.
    The reported p-value is for a one-sided hypothesis, to get the two-sided
    p-value multiply the returned p-value by 2.

    """
    x = np.asarray(x)
    y = np.asarray(y)
    n1 = len(x)
    n2 = len(y)
    ranked = rankdata(np.concatenate((x,y)))
    rankx = ranked[0:n1]       # get the x-ranks
    # ranky = ranked[n1:]        # the rest are y-ranks
    u1 = n1*n2 + (n1*(n1+1))/2.0 - np.sum(rankx,axis=0)  # calc U for x
    u2 = n1*n2 - u1                            # remainder is U for y
    bigu = max(u1,u2)
    smallu = min(u1,u2)
    # T = np.sqrt(tiecorrect(ranked))  # correction factor for tied scores
    T = tiecorrect(ranked)
    if T == 0:
        raise ValueError('All numbers are identical in amannwhitneyu')
    sd = np.sqrt(T*n1*n2*(n1+n2+1)/12.0)

    if use_continuity:
        # normal approximation for prob calc with continuity correction
        z = abs((bigu-0.5-n1*n2/2.0) / sd)
    else:
        z = abs((bigu-n1*n2/2.0) / sd)  # normal approximation for prob calc
    return smallu, distributions.norm.sf(z)  # (1.0 - zprob(z))


def ranksums(x, y):
    """
    Compute the Wilcoxon rank-sum statistic for two samples.

    The Wilcoxon rank-sum test tests the null hypothesis that two sets
    of measurements are drawn from the same distribution.  The alternative
    hypothesis is that values in one sample are more likely to be
    larger than the values in the other sample.

    This test should be used to compare two samples from continuous
    distributions.  It does not handle ties between measurements
    in x and y.  For tie-handling and an optional continuity correction
    see `scipy.stats.mannwhitneyu`.

    Parameters
    ----------
    x,y : array_like
        The data from the two samples

    Returns
    -------
    z-statistic : float
        The test statistic under the large-sample approximation that the
        rank sum statistic is normally distributed
    p-value : float
        The two-sided p-value of the test

    References
    ----------
    .. [1] http://en.wikipedia.org/wiki/Wilcoxon_rank-sum_test

    """
    x,y = map(np.asarray, (x, y))
    n1 = len(x)
    n2 = len(y)
    alldata = np.concatenate((x,y))
    ranked = rankdata(alldata)
    x = ranked[:n1]
    y = ranked[n1:]
    s = np.sum(x,axis=0)
    expected = n1*(n1+n2+1) / 2.0
    z = (s - expected) / np.sqrt(n1*n2*(n1+n2+1)/12.0)
    prob = 2 * distributions.norm.sf(abs(z))
    return z, prob


def kruskal(*args):
    """
    Compute the Kruskal-Wallis H-test for independent samples

    The Kruskal-Wallis H-test tests the null hypothesis that the population
    median of all of the groups are equal.  It is a non-parametric version of
    ANOVA.  The test works on 2 or more independent samples, which may have
    different sizes.  Note that rejecting the null hypothesis does not
    indicate which of the groups differs.  Post-hoc comparisons between
    groups are required to determine which groups are different.

    Parameters
    ----------
    sample1, sample2, ... : array_like
       Two or more arrays with the sample measurements can be given as
       arguments.

    Returns
    -------
    H-statistic : float
       The Kruskal-Wallis H statistic, corrected for ties
    p-value : float
       The p-value for the test using the assumption that H has a chi
       square distribution

    Notes
    -----
    Due to the assumption that H has a chi square distribution, the number
    of samples in each group must not be too small.  A typical rule is
    that each sample must have at least 5 measurements.

    References
    ----------
    .. [1] http://en.wikipedia.org/wiki/Kruskal-Wallis_one-way_analysis_of_variance

    """
    args = list(map(np.asarray, args))  # convert to a numpy array
    na = len(args)               # Kruskal-Wallis on 'na' groups, each in it's own array
    if na < 2:
        raise ValueError("Need at least two groups in stats.kruskal()")
    n = np.asarray(list(map(len, args)))

    alldata = np.concatenate(args)

    ranked = rankdata(alldata)  # Rank the data
    T = tiecorrect(ranked)      # Correct for ties
    if T == 0:
        raise ValueError('All numbers are identical in kruskal')

    # Compute sum^2/n for each group and sum
    j = np.insert(np.cumsum(n), 0, 0)
    ssbn = 0
    for i in range(na):
        ssbn += square_of_sums(ranked[j[i]:j[i+1]]) / float(n[i])

    totaln = np.sum(n)
    h = 12.0 / (totaln * (totaln + 1)) * ssbn - 3 * (totaln + 1)
    df = na - 1
    h = h / float(T)
    return h, chisqprob(h, df)


def friedmanchisquare(*args):
    """
    Computes the Friedman test for repeated measurements

    The Friedman test tests the null hypothesis that repeated measurements of
    the same individuals have the same distribution.  It is often used
    to test for consistency among measurements obtained in different ways.
    For example, if two measurement techniques are used on the same set of
    individuals, the Friedman test can be used to determine if the two
    measurement techniques are consistent.

    Parameters
    ----------
    measurements1, measurements2, measurements3... : array_like
        Arrays of measurements.  All of the arrays must have the same number
        of elements.  At least 3 sets of measurements must be given.

    Returns
    -------
    friedman chi-square statistic : float
        the test statistic, correcting for ties
    p-value : float
        the associated p-value assuming that the test statistic has a chi
        squared distribution

    Notes
    -----
    Due to the assumption that the test statistic has a chi squared
    distribution, the p-value is only reliable for n > 10 and more than
    6 repeated measurements.

    References
    ----------
    .. [1] http://en.wikipedia.org/wiki/Friedman_test

    """
    k = len(args)
    if k < 3:
        raise ValueError('\nLess than 3 levels.  Friedman test not appropriate.\n')
    n = len(args[0])
    for i in range(1,k):
        if len(args[i]) != n:
            raise ValueError('Unequal N in friedmanchisquare.  Aborting.')

    # Rank data
    data = _support.abut(*args)
    data = data.astype(float)
    for i in range(len(data)):
        data[i] = rankdata(data[i])

    # Handle ties
    ties = 0
    for i in range(len(data)):
        replist, repnum = find_repeats(array(data[i]))
        for t in repnum:
            ties += t*(t*t-1)
    c = 1 - ties / float(k*(k*k-1)*n)

    ssbn = pysum(pysum(data)**2)
    chisq = (12.0 / (k*n*(k+1)) * ssbn - 3*n*(k+1)) / c
    return chisq, chisqprob(chisq,k-1)


#####################################
####  PROBABILITY CALCULATIONS  ####
#####################################

zprob = special.ndtr


def chisqprob(chisq, df):
    """
    Probability value (1-tail) for the Chi^2 probability distribution.

    Broadcasting rules apply.

    Parameters
    ----------
    chisq : array_like or float > 0

    df : array_like or float, probably int >= 1

    Returns
    -------
    chisqprob : ndarray
        The area from `chisq` to infinity under the Chi^2 probability
        distribution with degrees of freedom `df`.

    """
    return special.chdtrc(df,chisq)

ksprob = special.kolmogorov
fprob = special.fdtrc


def betai(a, b, x):
    """
    Returns the incomplete beta function.

    I_x(a,b) = 1/B(a,b)*(Integral(0,x) of t^(a-1)(1-t)^(b-1) dt)

    where a,b>0 and B(a,b) = G(a)*G(b)/(G(a+b)) where G(a) is the gamma
    function of a.

    The standard broadcasting rules apply to a, b, and x.

    Parameters
    ----------
    a : array_like or float > 0

    b : array_like or float > 0

    x : array_like or float
        x will be clipped to be no greater than 1.0 .

    Returns
    -------
    betai : ndarray
        Incomplete beta function.

    """
    x = np.asarray(x)
    x = np.where(x < 1.0, x, 1.0)  # if x > 1 then return 1.0
    return special.betainc(a, b, x)

#####################################
#######  ANOVA CALCULATIONS  #######
#####################################


_msg = """`glm` is deprecated in scipy 0.13.0 and will be removed in 0.14.0.
Use `ttest_ind` for the same functionality in scipy.stats, or `statsmodels.OLS`
for a more full-featured general linear model."""
@np.deprecate_with_doc(_msg)
def glm(data, para):
    """
    Calculates a linear model fit ...
    anova/ancova/lin-regress/t-test/etc. Taken from:

    Returns
    -------
    statistic, p-value ???

    References
    ----------
    Peterson et al. Statistical limitations in functional neuroimaging
    I. Non-inferential methods and statistical models.  Phil Trans Royal Soc
    Lond B 354: 1239-1260.

    """
    if len(para) != len(data):
        raise ValueError("data and para must be same length in aglm")
    n = len(para)
    p = _support.unique(para)
    x = zeros((n,len(p)))  # design matrix
    for l in range(len(p)):
        x[:,l] = para == p[l]
    # fixme: normal equations are bad. Use linalg.lstsq instead.
    b = dot(dot(linalg.inv(dot(np.transpose(x),x)),  # i.e., b=inv(X'X)X'Y
                    np.transpose(x)),data)
    diffs = (data - dot(x,b))
    s_sq = 1./(n-len(p)) * dot(np.transpose(diffs), diffs)

    if len(p) == 2:  # ttest_ind
        c = array([1,-1])
        df = n-2
        fact = np.sum(1.0/np.sum(x,0),axis=0)  # i.e., 1/n1 + 1/n2 + 1/n3 ...
        t = dot(c,b) / np.sqrt(s_sq*fact)
        probs = betai(0.5*df,0.5,float(df)/(df+t*t))
        return t, probs
    else:
        raise ValueError("only ttest_ind implemented")


def f_value_wilks_lambda(ER, EF, dfnum, dfden, a, b):
    """Calculation of Wilks lambda F-statistic for multivarite data, per
    Maxwell & Delaney p.657.
    """
    if isinstance(ER, (int, float)):
        ER = array([[ER]])
    if isinstance(EF, (int, float)):
        EF = array([[EF]])
    lmbda = linalg.det(EF) / linalg.det(ER)
    if (a-1)**2 + (b-1)**2 == 5:
        q = 1
    else:
        q = np.sqrt(((a-1)**2*(b-1)**2 - 2) / ((a-1)**2 + (b-1)**2 - 5))
    n_um = (1 - lmbda**(1.0/q))*(a-1)*(b-1)
    d_en = lmbda**(1.0/q) / (n_um*q - 0.5*(a-1)*(b-1) + 1)
    return n_um / d_en


def f_value(ER, EF, dfR, dfF):
    """
    Returns an F-statistic for a restricted vs. unrestricted model.

    Parameters
    ----------
    ER : float
         `ER` is the sum of squared residuals for the restricted model
          or null hypothesis

    EF : float
         `EF` is the sum of squared residuals for the unrestricted model
          or alternate hypothesis

    dfR : int
          `dfR` is the degrees of freedom in the restricted model

    dfF : int
          `dfF` is the degrees of freedom in the unrestricted model

    Returns
    -------
    F-statistic : float

    """
    return ((ER-EF)/float(dfR-dfF) / (EF/float(dfF)))


def f_value_multivariate(ER, EF, dfnum, dfden):
    """
    Returns a multivariate F-statistic.

    Parameters
    ----------
    ER : ndarray
        Error associated with the null hypothesis (the Restricted model).
        From a multivariate F calculation.
    EF : ndarray
        Error associated with the alternate hypothesis (the Full model)
        From a multivariate F calculation.
    dfnum : int
        Degrees of freedom the Restricted model.
    dfden : int
        Degrees of freedom associated with the Restricted model.

    Returns
    -------
    fstat : float
        The computed F-statistic.

    """
    if isinstance(ER, (int, float)):
        ER = array([[ER]])
    if isinstance(EF, (int, float)):
        EF = array([[EF]])
    n_um = (linalg.det(ER) - linalg.det(EF)) / float(dfnum)
    d_en = linalg.det(EF) / float(dfden)
    return n_um / d_en


#####################################
#######  SUPPORT FUNCTIONS  ########
#####################################

def ss(a, axis=0):
    """
    Squares each element of the input array, and returns the sum(s) of that.

    Parameters
    ----------
    a : array_like
        Input array.
    axis : int or None, optional
        The axis along which to calculate. If None, use whole array.
        Default is 0, i.e. along the first axis.

    Returns
    -------
    ss : ndarray
        The sum along the given axis for (a**2).

    See also
    --------
    square_of_sums : The square(s) of the sum(s) (the opposite of `ss`).

    Examples
    --------
    >>> from scipy import stats
    >>> a = np.array([1., 2., 5.])
    >>> stats.ss(a)
    30.0

    And calculating along an axis:

    >>> b = np.array([[1., 2., 5.], [2., 5., 6.]])
    >>> stats.ss(b, axis=1)
    array([ 30., 65.])

    """
    a, axis = _chk_asarray(a, axis)
    return np.sum(a*a, axis)


def square_of_sums(a, axis=0):
    """
    Sums elements of the input array, and returns the square(s) of that sum.

    Parameters
    ----------
    a : array_like
        Input array.
    axis : int or None, optional
        If axis is None, ravel `a` first. If `axis` is an integer, this will
        be the axis over which to operate. Defaults to 0.

    Returns
    -------
    square_of_sums : float or ndarray
        The square of the sum over `axis`.

    See also
    --------
    ss : The sum of squares (the opposite of `square_of_sums`).

    Examples
    --------
    >>> from scipy import stats
    >>> a = np.arange(20).reshape(5,4)
    >>> stats.square_of_sums(a)
    array([ 1600.,  2025.,  2500.,  3025.])
    >>> stats.square_of_sums(a, axis=None)
    36100.0

    """
    a, axis = _chk_asarray(a, axis)
    s = np.sum(a,axis)
    if not np.isscalar(s):
        return s.astype(float)*s
    else:
        return float(s)*s


def fastsort(a):
    """
    Sort an array and provide the argsort.

    Parameters
    ----------
    a : array_like
        Input array.

    Returns
    -------
    fastsort : ndarray of type int
        sorted indices into the original array

    """
    # TODO: the wording in the docstring is nonsense.
    it = np.argsort(a)
    as_ = a[it]
    return as_, it
