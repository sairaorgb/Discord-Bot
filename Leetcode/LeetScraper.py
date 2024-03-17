from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import time
import undetected_chromedriver as uc
import os
from selenium.common.exceptions import NoSuchElementException
#Shows the path where chromedriver is installed
s= Service('chromedriver.exe')
driver=uc.Chrome()

def Login():
    """Responsible for logging into the account provided in the settings file"""
    driver.get('https://leetcode.com/accounts/login/')
    time.sleep(5)
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
    '''
    with open('Answers.txt','a') as file:
        file.write("\n\n")
        file.write(f"--------------{j}--------------\n")
        for i in code:
            file.write(i.get_attribute("innerText"))
            file.write("\n")
    '''

def Get_Question(url : str) -> str:
    """It scrapes the question from the problem's page"""
    driver.get(url)
    time.sleep(5)
    element= driver.find_element(by=By.XPATH,value="//div[@data-track-load='description_content']")
    return element.get_attribute('innerText')
    
    '''
    with open('Questions.txt','a',encoding='utf-8') as file:
        file.write(element.get_attribute('innerText'))
        file.write('\n')
    '''

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
Login()