from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import time
import undetected_chromedriver as uc
import os
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys
import pyperclip
import Results
#Shows the path where chromedriver is installed
s= Service('chromedriver.exe')
driver=uc.Chrome()

def Login():
    """Responsible for logging into the account provided in the settings file"""
    driver.get('https://leetcode.com/accounts/login/')
    time.sleep(5)
    #Retrieves the username and password else it asks input the username and password
    Username=os.environ.get('Leet_Username')
    Password=os.environ.get('Leet_Password')
    if Username is None:
        Username=input("Enter Username: ")
    if Password is None:
        Password=input("Enter Password: ")
    login=driver.find_element(by=By.XPATH,value="//input[@name='login']")
    password=driver.find_element(by=By.XPATH,value="//input[@name='password']")
    time.sleep(3)
    login.send_keys(Username)
    time.sleep(3)
    password.send_keys(Password)
    time.sleep(3)
    login_but=driver.find_element(by=By.XPATH,value="//button[@id='signin_btn']")
    login_but.click()
    time.sleep(5)


def Get_Answers(url:str) -> str:
    """ Will Scrape the solution from any question url
     By clicking on the solutions tab and then clicking on the very first solution.
      It understands which languages to scrape by scraping the code boxes written on 
      the solutions tab and based on that it decides which class to scrape from """
    #Click on solution tab
    driver.get(url)
    time.sleep(5)
    element = driver.find_element(by=By.XPATH,value="//div[@class='normal absolute left-0 top-0 whitespace-nowrap font-normal' and text()='Solutions']")
    element.click()
    #Once in solution tab
    def ParseLanguages(set_word):
            languages=[]
            lang=["Java","Python","C","JavaScript","Go","TypeScript","Rust","Kotlin","PHP","Ruby","Swift","Dart","Scala","Elixir","Racket","Erlang","MySQL"]
            
            for line in set_word:
                    for word in line.split('\n'):
                        if word != '':
                            if word in ["Python3","Python ML"]:
                                languages.append("language-python")
                            elif word=="C#":
                                languages.append("language-csharp")
                            elif word=="C++":
                                languages.append("language-cpp")
                            elif word in lang:
                                languages.append("language-"+word.lower())
            return languages 
    #Get all the languages
    time.sleep(5)
    parent=driver.find_element(by=By.XPATH,value="//div[@class='flex w-full flex-col']")
    first_child=parent.find_element(by=By.XPATH,value="./*[1]")
    lang=first_child.find_elements(by=By.XPATH,value=".//div[@class='text-label-2 dark:text-dark-label-2 bg-fill-3 dark:bg-dark-fill-3 whitespace-nowrap rounded-[20px] px-2 py-[1px] text-xs']")
    Languages=set()
    for i in lang:
            Languages.add(i.get_attribute("innerText"))
    Languages=ParseLanguages(Languages)
    first_child.click()
    time.sleep(5)
    #The part responsible for answers in solution page
    button=driver.find_elements(by=By.XPATH,value="//*[@class='relative cursor-pointer px-3 py-3 text-label-4 dark:text-dark-label-4 hover:text-label-1 dark:hover:text-dark-label-1 EoHqa']")
    for element in button:
            # print('Clicking the button')
            element.click()
            time.sleep(2)
    

    return_str = ""

    for j in Languages: 
        code=driver.find_elements(by=By.XPATH,value=f"//code[@class='{j}']")
        
        return_str += "\n\n"
        return_str += f"--------------{j}--------------\n"
        for i in code:
            return_str += i.get_attribute("innerText")
            return_str += "\n"
    
    return return_str

def Get_Question(url : str) -> str:
    """It scrapes the question from the problem's page"""
    driver.get(url)
    time.sleep(5)
    element= driver.find_element(by=By.XPATH,value="//div[@data-track-load='description_content']")
    return element.get_attribute('innerText')

def Get_Urls():
    """It returns all the urls for questions on any page that groups together questions eg "Arrays"""
    titles=[]
    cleaned_title=[]
    elements = driver.find_elements(by=By.XPATH,value="//td[@label='Title']")
    for element in elements:
        title = element.get_attribute("value")
        titles.append(title)
    
    for line in titles:
        url=f"https://leetcode.com/problems/{'-'.join(line.split())}/"
        cleaned_title.append(url)
    
    return cleaned_title

def get_questions_for_tags (tag : str):
    driver.get(f"https://leetcode.com/tag/{tag}") #Replace the url with the url of the group of questions needed to be scraped
    time.sleep(5)
    cleaned_title = Get_Urls()

    return cleaned_title    

def get_questions_for_company (company: str): 
    driver.get(f"https://leetcode.com/company/{company}") #Replace the url with the url of the group of questions needed to be scraped
    time.sleep(5)
    cleaned_title = Get_Urls()

    return cleaned_title

def get_editorial_solution(url:str) -> str:
    """Returns the editorial solution as a string if present"""
    #Click on the editorial button
    driver.get(url)
    time.sleep(5)
    try:
        element = driver.find_element(by=By.XPATH,value="//div[@class='normal absolute left-0 top-0 whitespace-nowrap font-normal' and text()='Editorial']")
        element.click()
        time.sleep(5)
        Solution=driver.find_element(by=By.XPATH,value="//div[@class='FN9Jv WRmCx']").get_attribute("innerText")
    except NoSuchElementException:
        Solution=""
    return Solution
def Get_Solution_Stats(Question_url:str,Language:str,Solution:str):
    """Takes as input the url of the question, the language the solution is written in and the solution
       It returns a class of type AcceptedSolution or RejectedSolution
       It returns None if there was an error with loading the driver
       Constrains: Language has to be exactly as it appears in leetcode
                   Solutions must hold proper indentation if necessary for the langauge
       """
    driver.get(Question_url)
    time.sleep(10)
    #Clicks on the code button
    submit_button=driver.find_element(by=By.XPATH,value="//button[@class='rounded items-center whitespace-nowrap focus:outline-none inline-flex bg-transparent dark:bg-dark-transparent text-text-secondary dark:text-text-secondary active:bg-transparent dark:active:bg-dark-transparent hover:bg-fill-secondary dark:hover:bg-fill-secondary px-1.5 py-0.5 text-sm font-normal group']")
    submit_button.click()
    time.sleep(3)
    #Select the language
    Language_div = driver.find_element(By.XPATH, f"//div[text()='{Language}']")
    Language_div.click()
    time.sleep(3)
    #Selects the text area to input code
    txt=driver.find_element(by=By.XPATH,value="//textarea[@class='inputarea monaco-mouse-cursor-text']")
    txt.send_keys(Keys.CONTROL + 'a')
    txt.send_keys(Keys.DELETE) 
    time.sleep(3)
    pyperclip.copy(Solution)
    txt.send_keys(Keys.CONTROL + 'v') 
    time.sleep(3)
    txt.send_keys(Keys.CONTROL + Keys.ENTER)
    #Wait for solution to load
    time.sleep(20)
    try:
        submission_result=driver.find_element(by=By.XPATH,value="//span[@data-e2e-locator='submission-result']").text
        if submission_result == "Accepted":
            results=driver.find_element(by=By.XPATH,value="//div[@class='rounded-sd flex min-w-[275px] flex-1 cursor-pointer flex-col px-4 py-3 text-xs transition hover:opacity-100 bg-sd-accent']").text
            runtime_result=""
            for c in results:
                if c.isspace():
                    runtime_result +=" "
                else:
                    runtime_result +=c
            results=driver.find_element(by=By.XPATH,value="//div[@class='rounded-sd flex min-w-[275px] flex-1 cursor-pointer flex-col px-4 py-3 text-xs transition hover:opacity-100 opacity-40']").text
            memory_result=""
            for c in results:
                if c.isspace():
                    memory_result +=" "
                else:
                    memory_result +=c
            return Results.AcceptedSolution(runtime_result,memory_result)
    except NoSuchElementException:
        pass
    try:
        submission_result=driver.find_element(by=By.XPATH,value="//span[@data-e2e-locator='console-result']").text
        if submission_result =="Time Limit Exceeded":
            return Results.RejectedSolution("TimeError",submission_result)
        elif submission_result =="Memory Limit Exceeded":
            return Results.RejectedSolution("MemoryError",submission_result)
        elif submission_result == "Runtime Error":
            error=driver.find_element(by=By.XPATH,value="//div[@class='font-menlo whitespace-pre-wrap break-all text-xs text-red-60 dark:text-red-60']").text
            return Results.RejectedSolution("RuntimeError",error)
    
    except NoSuchElementException:
        pass
def Get_Solution_Template(Question_url:str,Language:str)->str:
    """Takes as input the url of the question,the langauge of the solution and returns the template of the solution"""
    driver.get(Question_url)
    time.sleep(10)
    #Clicks on the code button
    submit_button=driver.find_element(by=By.XPATH,value="//button[@class='rounded items-center whitespace-nowrap focus:outline-none inline-flex bg-transparent dark:bg-dark-transparent text-text-secondary dark:text-text-secondary active:bg-transparent dark:active:bg-dark-transparent hover:bg-fill-secondary dark:hover:bg-fill-secondary px-1.5 py-0.5 text-sm font-normal group']")
    submit_button.click()
    time.sleep(3)
    #Select the language
    Language_div = driver.find_element(By.XPATH, f"//div[text()='{Language}']")
    Language_div.click()
    time.sleep(3)
    #Selects the text area to input code
    txt=driver.find_element(by=By.XPATH,value="//textarea[@class='inputarea monaco-mouse-cursor-text']")
    txt.send_keys(Keys.CONTROL + 'a')
    txt.send_keys(Keys.CONTROL+'c')
    template=pyperclip.paste()
    return template
