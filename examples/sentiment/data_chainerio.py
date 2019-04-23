import codecs
import re

# CHAINERIO import
import chainerio
# chainerio.set_root("hdfs")
# CHAINERIO import end

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
    # CHAINERIO modify
    with chainerio.open(path, mode='r', encoding='utf-8') as f:
    # CHAINERIO modify end
            trees = []
            for line in f:
                line = line.strip()
                tree = SexpParser(line).parse()
                trees.append(tree)
                if max_size and len(trees) >= max_size:
                    break

    return trees
