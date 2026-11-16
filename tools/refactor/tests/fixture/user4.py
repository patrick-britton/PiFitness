from pkg_a.foo import hello as h

# This is a comment mentioning from pkg_a.foo import hello - must NOT be rewritten
MSG = "from pkg_a.foo import hello"  # this string must NOT be rewritten

if __name__ == "__main__":
    print(h())
    print(MSG)