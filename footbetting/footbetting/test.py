def gene():
    print '*'
    for i in range(1):
        print '&'
        yield i
        print '#'
    print '%'
    yield 11
    print '$'
for x in gene():
    print x