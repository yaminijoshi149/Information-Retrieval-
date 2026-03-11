import requests
import json
import time
import random
import csv
from bs4 import BeautifulSoup
from scipy.stats import spearmanr

# Load queries from the provided text file
queries_file = "100QueriesSet4.txt"
google_results_file = "Google_Result4.json"
output_json_file = "hw1.json"
output_csv_file = "hw1.csv"

# Load queries
with open(queries_file, "r") as f:
    queries = [line.strip() for line in f.readlines()]

# Load Google reference results
with open(google_results_file, "r") as f:
    google_results = json.load(f)

# Function to scrape Bing search results
def scrape_bing(query):
    search_url = f"https://www.bing.com/search?q={query}"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(search_url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    
    results = []
    for result in soup.find_all("li", attrs={"class": "b_algo"}):
        link = result.find("a")
        if link and link.get("href") and len(results) < 10:
            results.append(link.get("href"))
    
    return results

# Scrape results with delays
search_results = {}
for query in queries:
    search_results[query] = scrape_bing(query)
    time.sleep(random.randint(10, 100))  # Random delay to prevent blocking

# Save scraped results to JSON
with open(output_json_file, "w") as f:
    json.dump(search_results, f, indent=4)

# Function to calculate overlap percentage and count
def compute_overlap(google_urls, scraped_urls):
    matches = set(google_urls) & set(scraped_urls)
    return len(matches), (len(matches) / len(google_urls) * 100 if google_urls else 0)

# Function to compute Spearman’s rank correlation
def compute_spearman(google_urls, scraped_urls):
    common_urls = [url for url in scraped_urls if url in google_urls]
    if len(common_urls) == 0:
        return 0  # No correlation if no matches
    if len(common_urls) == 1:
        return 1 if google_urls.index(common_urls[0]) == scraped_urls.index(common_urls[0]) else 0
    
    google_ranks = [google_urls.index(url) + 1 for url in common_urls]
    scraped_ranks = [scraped_urls.index(url) + 1 for url in common_urls]
    
    return spearmanr(google_ranks, scraped_ranks)[0]

# Compute overlap, count, and Spearman coefficient for each query
results = []
total_overlap_count = 0
total_overlap_percentage = 0
total_spearman = 0
for query in queries:
    google_urls = google_results.get(query, [])[:10]
    scraped_urls = search_results.get(query, [])
    overlap_count, overlap = compute_overlap(google_urls, scraped_urls)
    spearman = compute_spearman(google_urls, scraped_urls)
    results.append([query, overlap_count, overlap, spearman])
    total_overlap_count += overlap_count
    total_overlap_percentage += overlap
    total_spearman += spearman

# Compute averages
num_queries = len(queries)
avg_overlap_count = total_overlap_count / num_queries if num_queries else 0
avg_overlap_percentage = total_overlap_percentage / num_queries if num_queries else 0
avg_spearman = total_spearman / num_queries if num_queries else 0

# Save results to CSV
with open(output_csv_file, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Query", "Overlap Count", "Percent Overlap", "Spearman Coefficient"])
    writer.writerows(results)
    writer.writerow(["Averages", avg_overlap_count, avg_overlap_percentage, avg_spearman])

print("Scraping and analysis completed. Results saved as hw1.json and hw1.csv.")
print(f"Average Overlap Count: {avg_overlap_count}")
print(f"Average Percent Overlap: {avg_overlap_percentage}%")
print(f"Average Spearman Coefficient: {avg_spearman}")


# import requests
# import json
# import time
# import random
# import csv
# from bs4 import BeautifulSoup
# from scipy.stats import spearmanr

# # Load queries from the provided text file
# queries_file = "testing.txt"
# google_results_file = "test.json"
# output_json_file = "hw1_testing.json"
# output_csv_file = "hw1_testing.csv"

# # Load queries
# with open(queries_file, "r") as f:
#     queries = [line.strip() for line in f.readlines()]

# # Load Google reference results
# with open(google_results_file, "r") as f:
#     google_results = json.load(f)

# # Function to scrape DuckDuckGo search results
# def scrape_duckduckgo(query):
#     search_url = f"https://www.duckduckgo.com/html/?q={query}"
#     headers = {"User-Agent": "Mozilla/5.0"}
#     response = requests.get(search_url, headers=headers)
#     soup = BeautifulSoup(response.text, "html.parser")
    
#     results = []
#     for link in soup.find_all("a", attrs={"class": "result__a"}):
#         url = link.get("href")
#         if url and len(results) < 10:  # Collect up to 10 results
#             results.append(url)
    
#     return results

# # Scrape results with delays
# search_results = {}
# for query in queries:
#     search_results[query] = scrape_duckduckgo(query)
#     time.sleep(random.randint(10, 100))  # Random delay to prevent blocking

# # Save scraped results to JSON
# with open(output_json_file, "w") as f:
#     json.dump(search_results, f, indent=4)

# # Function to calculate overlap percentage
# def compute_overlap(google_urls, scraped_urls):
#     matches = set(google_urls) & set(scraped_urls)
#     return len(matches) / len(google_urls) * 100 if google_urls else 0

# # Function to compute Spearman’s rank correlation
# def compute_spearman(google_urls, scraped_urls):
#     common_urls = [url for url in scraped_urls if url in google_urls]
#     if len(common_urls) == 0:
#         return 0  # No correlation if no matches
#     if len(common_urls) == 1:
#         return 1 if google_urls.index(common_urls[0]) == scraped_urls.index(common_urls[0]) else 0
    
#     google_ranks = [google_urls.index(url) + 1 for url in common_urls]
#     scraped_ranks = [scraped_urls.index(url) + 1 for url in common_urls]
    
#     return spearmanr(google_ranks, scraped_ranks)[0]

# # Compute overlap and Spearman coefficient for each query
# results = []
# for query in queries:
#     google_urls = google_results.get(query, [])[:10]
#     scraped_urls = search_results.get(query, [])
#     overlap = compute_overlap(google_urls, scraped_urls)
#     spearman = compute_spearman(google_urls, scraped_urls)
#     results.append([query, overlap, spearman])

# # Save results to CSV
# with open(output_csv_file, "w", newline="") as f:
#     writer = csv.writer(f)
#     writer.writerow(["Query", "Percent Overlap", "Spearman Coefficient"])
#     writer.writerows(results)

# print("Scraping and analysis completed. Results saved as hw1.json and hw1.csv.")

