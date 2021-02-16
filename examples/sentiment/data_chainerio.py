import codecs
import re

# PFIO import
import pfio

# pfio.set_root("hdfs")
# PFIO import end

class SexpParser(object):

    def __init__(self, line):
        self.tokens = re.findall(r'\(|\)|[^\(\) ]+', line)
        self.pos = 0

    def parse(self):
        assert self.pos < len(self.tokens)
        token = self.tokens[self.pos]
        assert token != ')'
        self.pos += 1

        if token == '(':
            children = []
            while True:
                assert self.pos < len(self.tokens)
                if self.tokens[self.pos] == ')':
                    self.pos += 1
                    break
                else:
                    children.append(self.parse())
            return children
        else:
            return token


def read_corpus(path, max_size):
    # PFIO modify
    with pfio.open(path, mode='r', encoding='utf-8') as f:
    # PFIO modify end
            trees = []
            for line in f:
                line = line.strip()
                tree = SexpParser(line).parse()
                trees.append(tree)
                if max_size and len(trees) >= max_size:
                    break

    return trees
