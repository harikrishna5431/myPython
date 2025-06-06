import io
from docx import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import FAISS
from langchain_community.embeddings import SentenceTransformerEmbeddings
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain

def parse_word_document(file):
    """
    Parses a .docx file, extracting sections based on headings.
    A section is a heading followed by its content (paragraphs).
    """
    doc_sections = {}
    try:
        # Use io.BytesIO to handle the uploaded file in memory
        doc = Document(io.BytesIO(file.read()))
        current_heading = "Introduction"  # Default for content before first heading
        current_content = []

        for para in doc.paragraphs:
            # A heading style is used to identify a new section
            if para.style.name.startswith('Heading'):
                # When a new heading is found, save the previous section
                if current_heading and current_content:
                    doc_sections[current_heading] = "\n".join(current_content)
                
                # Start a new section
                current_heading = para.text
                current_content = []
            else:
                # Add paragraph to the current section's content
                if para.text.strip(): # Avoid adding empty lines
                    current_content.append(para.text)
        
        # Add the last section after the loop
        if current_heading and current_content:
            doc_sections[current_heading] = "\n".join(current_content)
            
    except Exception as e:
        print(f"Error parsing document: {e}")
        return {}
        
    return doc_sections


def create_vector_store(doc_sections, embedding_model):
    """
    Creates a FAISS vector store from the document sections.
    """
    # Combine all section content into a single list of texts for processing
    all_text = list(doc_sections.values())
    
    if not all_text:
        return None

    # Use a text splitter for consistency, though sections are already separated
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000, 
        chunk_overlap=100
    )
    docs = text_splitter.create_documents(all_text)

    try:
        # Create the vector store using FAISS
        vector_store = FAISS.from_documents(docs, embedding_model)
        return vector_store
    except Exception as e:
        print(f"Error creating vector store: {e}")
        return None

def get_comparison_feedback(template_sections, vector_store, llm):
    """
    Compares template sections to the document's vector store and gets LLM feedback.
    """
    feedback_report = []

    # Define the prompt template for the LLM
    prompt_template = PromptTemplate(
        input_variables=["template_section", "template_content", "retrieved_content"],
        template="""
        You are an expert document analyzer. Your task is to compare a section from a template document to the most relevant content found in a second document.

        **Template Section Name:**
        {template_section}

        **Template Section Description/Content:**
        "{template_content}"

        **Most Relevant Content Found in Second Document:**
        "{retrieved_content}"

        **Your Analysis:**
        1.  **Presence:** Based on the retrieved content, is this section present in the second document? (Answer with "Found" or "Potentially Missing").
        2.  **Relevance Score:** On a scale of 1 to 10, how relevant is the retrieved content to the template's description? (1 = Not relevant, 10 = Perfectly relevant).
        3.  **Feedback:** Provide a concise, bullet-pointed feedback on what the second document's content covers and what it might be missing based on the template.
        """
    )

    # Create an LLM chain
    chain = LLMChain(llm=llm, prompt=prompt_template)
    
    # Iterate through each section of the template
    for section, content in template_sections.items():
        if vector_store:
            # Perform a similarity search in the vector store for the current section's content
            # k=1 retrieves the single most similar chunk
            retrieved_docs = vector_store.similarity_search(content, k=1)
            retrieved_content = retrieved_docs[0].page_content if retrieved_docs else "No relevant content found."
        else:
            retrieved_content = "No content available in the document to compare."

        # Run the LLM chain to get the analysis
        try:
            response = chain.invoke({
                "template_section": section,
                "template_content": content,
                "retrieved_content": retrieved_content
            })
            feedback_report.append({
                "section": section,
                "analysis": response['text']
            })
        except Exception as e:
            feedback_report.append({
                "section": section,
                "analysis": f"Error during LLM analysis: {e}"
            })
            
    return feedback_report

