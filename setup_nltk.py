import nltk
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

def download_nltk_data():
    """Download required NLTK data packages."""
    required_packages = [
        'punkt',
        'vader_lexicon',
        'averaged_perceptron_tagger',
        'wordnet'
    ]
    
    print("Downloading NLTK data packages...")
    for package in required_packages:
        try:
            nltk.download(package, quiet=True)
            print(f"✓ Successfully downloaded {package}")
        except Exception as e:
            print(f"✗ Failed to download {package}: {str(e)}")

if __name__ == "__main__":
    download_nltk_data() 