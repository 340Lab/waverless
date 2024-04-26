import random
import wordlist
import os

CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
os.chdir(CUR_FDIR)


def generate_random_word(word_map):
    return random.choice(word_map)

def generate_random_words(file_path, file_size, word_map, seed=None):
    # Check if the file exists, create it if not
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    if not os.path.exists(file_path):
        with open(file_path, 'w') as file:
            pass  # Create an empty file
    else:
        return
    
    random.seed(seed)
    len_=0
    with open(file_path, 'w') as file:
        for _ in range(file_size):
            word = generate_random_word(word_map)
            file.write(word + '\n')
            len_+=len(word)+1;
            if len_ >= file_size:
                break


if __name__ == "__main__":
    file_path = "test_dir/files/random_words.txt"
    file_size = 1000000000  # 1 billion words
    seed_value = 42  # Adjust the seed value as needed

    #return if file already exists
    if os.path.exists(file_path):
        print(f"\nFile {file_path} already exists. Exiting.\n")
        exit(0)

    # Example: Use a built-in English word list
    generator = wordlist.Generator('en')  # Use 'en' for English words
    english_words=[]
    for w in generator.generate(2,10):
        english_words.append(w)
        if len(english_words) >= 3000:
            break
    
    generate_random_words(file_path, file_size, english_words, seed=seed_value)
    print(f"Random words generated and written to {file_path}. Seed: {seed_value}.")
