import requests
import json
import polars as pl
import os
import base64
import time
from concurrent.futures import ThreadPoolExecutor
import argparse

help_str = """This is a command line tool to scrape ratemyprofessor.com
Example 1
************************************
python ScrapeRMP.py -get_profs <start> <end> <out>

ratemyprofessor professors are associated with schools.  Schools have an id and the ids are, roughly, sequential.  https://www.ratemyprofessors.com/school/298 - in that url 298 is the school id.

Give a start and end school_id and collect all professors at all schools in the range and save them to the file specified by <out>.  CAREFUL!  This will overwrite whatever is at the file specified by out.

<start> is the starting id you want to start scraping with.
<end> is the ending id you want to stop scraping with.
<out> is the filename for a CSV that you want to create containing the professor data.

python ScrapeRMP.py -get_profs 0 300 Profs.csv
will check the 300 ids in that range.  Not all of them will be schools, but many will be.
For each school the tool will get all professors associated with that school and the data from this scrape will be stored in Profs.csv

Example 2
************************************
python ScrapeRMP.py -get_reviews <profs> <out>

Collect all review information for all provided professors.  The professors are provided as a CSV file with a column labeled "id" - the <profs> paramater .  Review data will be stored in a csv with the filename indicated by the <out> parameter.

python ScrapeRMP.py -get_reviews Profs.csv Reviews.csv
This will read Profs.csv, extract the professor id values, then collect all review information and save it to Reviews.csv.
"""

# Graphql queries to interact with ratemyprofessor.com
prof_query = open(f"Queries{os.sep}ProfReviews.gql").read()
search_query = open(f"Queries{os.sep}ProfsAtSchool.gql").read()
pql = open(f"Queries{os.sep}ProfReviews.gql").read()


def school_valid(school_number: int):
    """
    Use a get request to check if ratemyprofessor.com/school_number is a valid school or not.
    A school is considered invalid if the html response contains an error message or if the school has no ratings and valid otherwise.

    Args:
        school_number (int): Numeric id of the school

    Returns:
        bool: True if the school is valid, False otherwise.
    """

    resp = requests.get(f"https://www.ratemyprofessors.com/school/{school_number}")
    error_message = "We couldn&#x27;t find the school you were looking for" in resp.text
    no_ratings = "have any ratings yet" in resp.text
    return not (error_message or no_ratings)


def get_profs(school_number: int, page_size=1_000, pages=None):
    """
    Retrieve a list of professors associated with a school from ratemyprofessor.com.

    ratemyprofessor.com has separate pages for schools, with many professors associated with each school.
    The site uses sequential IDs with a prefix, i.e., "School-{school_num}", which are then encoded in base64.
    Some schools may be "tarpits" meant to deter web scraping. To avoid these, we first verify the school's validity before retrieving associated professors.

    Args:
        school_number (int): The numeric ID of the school, which will be transformed to a base64 encoded school ID for the request.
        page_size (int)=1_000: Number of professors to fetch at once.  Defaults to a value which my testing shows is most efficient.
        pages=None: If an int is supplied then only at most this many pages will be fetched and returned.

    Returns:
        list[dict]: A list of professor objects associated with the specified school.  If school is invalid will return empty list.

    Example:
        >>> get_professors(1234)
        [{'firstName': 'John', 'lastName': 'Doe', 'avgDifficulty': 4.5, 'department': 'Computer Science'}, ...]

    """
    
    valid = school_valid(school_number)
    if not valid:
        return []

    with requests.session() as session:
        # These values come from making the request in the browser at https://www.ratemyprofessors.com/school/298
        headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Authorization': 'Basic dGVzdDp0ZXN0',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
        }
        cur_cursor = ""
        
        school_id = base64.b64encode(f"School-{school_number}".encode()).decode()
        results = []
        has_more = True
        pages_so_far = 0
        
        while has_more:
            variables = {"count":page_size,"cursor":cur_cursor,"query":{"text":"","schoolID":school_id,"fallback":False,"departmentID":None}}
            ql = {
                "query": search_query,
                "variables": variables
            }
            
            response = session.post('https://www.ratemyprofessors.com/graphql', headers=headers, data=json.dumps(ql))
    
            try:
                j = response.json()
                page_info = j["data"]["search"]["teachers"]["pageInfo"]
                edges = j['data']['search']['teachers']['edges']
            except requests.exceptions.RequestException as e:
                print(j["errors"])
                print(f"Request error: {e}", search_query, variables)
                return results
            except json.JSONDecodeError as e:
                print(f"JSON Decode Error: {e}", response.status_code, response.text, search_query, variables)
                return results
            except KeyError as e:
                print(f"Request error: {e}", search_query, variables, j)
                return results
                
            for edge in edges:
                node = edge["node"]
                if not node['__typename'] == 'Teacher':
                    continue
                node["schoolId"] = node["school"]["id"]
                node["schoolName"] = node["school"]["name"]
                del node["school"]
                results.append(node)
                
            has_more = page_info['hasNextPage']
            cur_cursor = page_info['endCursor']
            pages_so_far += 1
            if pages is not None and pages_so_far >= pages:
                break
    
    return results


def all_profs(start: int, end:int, save=None, save_interval=None):
    """
    For all school numbers between start and end get all professors at each school.
    If save is a filename, then write professors to that file.
    """
    
    result = []

    with ThreadPoolExecutor(max_workers=5) as tpe:
        futures = []
        for i in range(start, end):
            futures.append(tpe.submit(get_profs, i))

        for i, f in enumerate(futures):
            try:
                result += f.result(timeout=30)
            except Exception as e:
                print(i, e)
                continue
            print(f"{i+start}/{end}", end="\r")

    if save is not None:
        print(len(result))
        df = pl.DataFrame(result)
        df = df.unique()
        df.write_csv(save)
        
    return result
        

def get_prof_reviews(prof_id, num_reviews=100):
    """
    Get all the ratemyprofessor.com reviews for a specific professor by id.  

    Args:
        prof_id: str - base64 encoded professor id should look like: VGVhY2hlci0zMDczMjA=
        num_reviews=100: int - number of reviews associated with professor, this is so we can minimize request number/time by setting page_size.

    Returns:
        {'data': [review dicts], 'errors': [list of prof ids that resulted in errors]}
    """
    cur_cursor = ""
    keep_going = True
    results = []

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Authorization': 'Basic dGVzdDp0ZXN0',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
    }

    with requests.session() as session:
        while keep_going:
            ql = {
                "query": pql,
                "variables": {"count": min(1_000, num_reviews), "id": prof_id, "courseFilter": None, "cursor": cur_cursor}
            }
        
            try:
                response = session.post('https://www.ratemyprofessors.com/graphql', headers=headers, data=json.dumps(ql))
                rj = response.json()
                nodes = rj["data"]["node"]["ratings"]["edges"]
                for node in nodes:
                    node = node["node"]
                    node['profId'] = prof_id
                    del node["thumbs"]
                    if "teacherNote" in node:
                        del node["teacherNote"]
                    results.append(node)
    
                page_info = rj["data"]["node"]["ratings"]["pageInfo"]
                keep_going = page_info["hasNextPage"]
                cur_cursor = page_info["endCursor"]
    
            except Exception as e:
                print(f"Error fetching reviews for professor {prof_id}: {e}")
                print("Sleeping 20 to prevent rate limiting")
                time.sleep(20)
                return {"data": results, "error": True}
                

    return {"data": results, "error": False}


def get_all_prof_reviews(prof_ids, prof_num_reviews):
    results = []
    errors = []
    count = 0
    with ThreadPoolExecutor(max_workers=5) as tpe:
        futures = []
        for i in range(len(prof_ids)):
            futures.append(tpe.submit(get_prof_reviews, prof_ids[i], prof_num_reviews[i]))

        for i, future in enumerate(futures):
            pid = prof_ids[i]
            print(f"{str(i).zfill(7)}", end="\r")
            try:
                res = future.result(timeout=20)
                nr, ne = res["data"], res["error"]
                results.extend(nr)
                if ne:
                    errors.append(pid)
                    
            except Exception as e:
                errors.append(pid)
                print(f"Error processing professor {future} {pid}: {e}.  Sleeping 20.")
                time.sleep(20)

    return {"data": results, "errors": errors}


def all_prof_reviews(prof_ids, prof_num_reviews, batch_size=1_000_000, out_name="Reviews10"):
    keep_going = True
    count = 0
    errors = []
    total_data = []
    
    while keep_going:
        print(f"On batch {count}/{len(prof_ids) // batch_size}")
        start, end = count * batch_size, (count + 1) * batch_size
        batch_ids, batch_nums = prof_ids[start:end], prof_num_reviews[start:end]
        keep_going = start < len(prof_ids)

        results = get_all_prof_reviews(batch_ids, batch_nums)
        data, ne = results["data"], results["errors"]
        errors += ne
        total_data += data

        """
        df = pl.DataFrame(data)
        df = df.unique()
        df.write_csv(f"{out_name}-{count}.csv")
        """
        
        count += 1

    df = pl.DataFrame(total_data)
    df = df.unique()
    df.write_csv(out_name)

    return errors
    

def parse_arguments():
    parser = argparse.ArgumentParser(
        prog='ScrapeRMP',
        description='Get data from ratemyprofessor.com',
        epilog='Have a nice day!')

    parser.add_argument(
        '--get_profs', 
        help="Example: --get_profs <start> <end> <filename_to_save_to>",
        nargs=3,
    )

    parser.add_argument(
        '--get_reviews', 
        help="Example: --get_reviews <Profs.csv> <filename_to_save_to>",
        nargs=2,
    )
    
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    if args.get_profs:
        start = int(args.get_profs[0])
        end = int(args.get_profs[1])
        filename = args.get_profs[2]

        print(f"Getting professors from schools between ({start}, {end})")
        all_profs(start=start, end=end, save=filename)
        
    elif args.get_reviews:
        profs_csv = args.get_reviews[0]
        reviews_out = args.get_reviews[1]

        print("Getting reviews!")

        profs_csv = pl.read_csv(profs_csv)
        prof_ids = profs_csv["id"].to_list()
        num_reviews = profs_csv["numRatings"].to_list()
        print(f"Getting reviews for {len(prof_ids)} professors.")
        all_prof_reviews(prof_ids, num_reviews, out_name=reviews_out)
        
    else:
        print(help_str)
    
