import spacy
import numpy as np
nlp = spacy.load("en_core_web_md")

vec = nlp.vocab["software Developer"].vector
print(vec)


# your_word = "Python Developer"
#
# ms = nlp.vocab.vectors.most_similar(
#     np.asarray([nlp.vocab.vectors[nlp.vocab.strings[your_word]]]), n=10)
#
# print(ms)