import os
import google.generativeai as genai
import textwrap
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
model = genai.GenerativeModel('gemini-pro')
def to_markdown(text):
  
  """Converts text to a markdown code block.
  Returns:
      A string containing the text formatted as a markdown code block.
  """
  text = text.replace('•', '  *')
  return f"`\n{textwrap.indent(text, '> ', predicate=lambda _: True)}\n`"
extracted_text=""""""#Replace with the text from which you want to extract data
prompt="The provided text is extracted from an image of the job listing.From the provided text extract out {}.Only provide from the text given."
required_fields=["Skills required","Vacancies","Location"]#Add in the fields you need from the text
output=[]#Stores all the results
for field in required_fields:
    final_prompt=prompt.format(field)
    response=model.generate_content([extracted_text,final_prompt],stream=True)
    response.resolve()
    output.append(to_markdown(response.text))

    