import nltk
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

def ensure_nltk_resources():
    """
    Fetch all NLTK resources required for airline customer review analysis.
    This includes sentiment, tokenization, and stopword modules.
    """
    resources = [
        'punkt',
        'vader_lexicon',
        'averaged_perceptron_tagger',
        'wordnet',
        'stopwords',
    ]
    print("Ensuring NLTK resources are available...")
    for res in resources:
        try:
            nltk.download(res, quiet=True)
            print(f"[NLTK] {res} ✓")
        except Exception as exc:
            print(f"[NLTK] {res} ✗ ({exc})")

if __name__ == "__main__":
    ensure_nltk_resources()