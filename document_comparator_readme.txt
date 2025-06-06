Of course. Here is a step-by-step guide on how to compare two Word documents using embeddings and a large language model (LLM), with a final user interface built with Streamlit. This approach will allow you to treat the first document as a template and evaluate the second for missing sections and content relevance.
Step 1: Upload and Define the Template
First, you'll need a way to upload the template Word document. In a Python environment, you can use the python-docx library to read the file. The key is to parse the template not just as a whole, but by its distinct sections as you've described (headers, paragraphs, tables, etc.).
You'll want to create a structured representation of the template. This can be a JSON object or a Python dictionary where each key represents a section (e.g., "Introduction," "Methodology") and the value is the description or content of that section. This structure is what you'll use for comparison.
Step 2: Parse and Embed the Template
Once you have the text content for each section of your template, you can proceed with creating embeddings.
 * Document Parser: Use a document parser like python-docx to extract text from headers, paragraphs, and tables. For images, you can extract associated captions or surrounding text.
 * Chunking: Breaking the text into smaller pieces is crucial for effective embedding. A chunk_size of 100 tokens with chunk_overlap of 0 is a reasonable starting point, but you may need to adjust this based on the nature of your content. For section-based analysis, it's often better to treat each section as a single "chunk" if its content is not too long.
 * Embedding: You can use Google's embeddings or any other state-of-the-art model to convert these text chunks into numerical vectors.
Step 3: Initialize Open-Source Embeddings
While you mentioned Google's embeddings, for local development or cost-saving purposes, you can use open-source models. The sentence-transformers library is an excellent choice for this. A popular model like 'all-MiniLM-L6-v2' is lightweight and effective for semantic similarity tasks.
from sentence_transformers import SentenceTransformer
embedding_model = SentenceTransformer('all-MiniLM-L6-v2')

You would then use embedding_model.encode(chunks) to generate the embeddings for your text chunks.
Step 4: Process and Embed the Second Document
This step mirrors Step 2 but is applied to the second Word document (the one you want to evaluate).
 * Upload: Use the same file upload mechanism for the second document.
 * Parse and Chunk: Apply the same parsing and chunking strategy. It's important to be consistent in how you process both documents to ensure a fair comparison.
 * Embed and Store: Generate embeddings for the chunks of the second document and store them in a vector store like FAISS or ChromaDB. This allows for efficient similarity searching.
Step 5: Create a Prompt Template and Perform Comparison with LangChain
This is where the LLM does the heavy lifting. LangChain is a powerful framework for building LLM-powered applications.
 * Similarity Search: For each section in your template, take its embedding and perform a similarity search against the vector store of the second document. This will retrieve the most relevant chunks from the second document for that particular section.
 * Prompt Template: Create a well-defined prompt template. This guides the LLM on how to perform the comparison. The prompt should include:
   * The template section's description.
   * The content of the most relevant chunks from the second document.
   * A clear instruction to evaluate if the section is present and how well the content matches the description.
Here’s an example of what the prompt might look like:
You are a document analysis assistant. Based on the template section description and the provided content from the document being reviewed, please perform the following:
1.  Determine if this section is present in the document.
2.  Assess the relevance of the provided content to the section description on a scale of 1 to 10.
3.  Provide a brief feedback on what is missing or how it could be improved.

**Template Section:** "Project Overview - This section should provide a high-level summary of the project goals, scope, and key deliverables."

**Retrieved Content from Document 2:** "This document outlines the primary objectives for the Q3 initiative. We will focus on market expansion and product development. The main outcomes will be a new software module and a report on user engagement."

Your analysis:

 * LangChain Chain: Use a LangChain "chain" (like LLMChain or a more complex custom chain) to automate the process of feeding this prompt to the LLM for each section of your template.
Step 6: Create a Streamlit UI
Streamlit is an excellent choice for creating a simple and interactive web application for this task.
 * File Uploaders: Use st.file_uploader to allow users to upload both the template and the document to be analyzed.
 * Processing Button: A button to trigger the comparison process.
 * Displaying Feedback: Organize the feedback in a clear and readable format. You could use st.expander for each section of the template. Inside each expander, you can display:
   * The template section description.
   * A status (e.g., "Found," "Missing").
   * The relevance score.
   * The LLM's qualitative feedback.
This user-friendly interface will make the results of your complex analysis pipeline easy to understand for any user.

from sentence_transformers import SentenceTransformer
embedding_model = SentenceTransformer('all-MiniLM-L6-v2')


   Of course. Here is a step-by-step guide on how to compare two Word documents using embeddings and a large language model (LLM), with a final user interface built with Streamlit. This approach will allow you to treat the first document as a template and evaluate the second for missing sections and content relevance.
Step 1: Upload and Define the Template
First, you'll need a way to upload the template Word document. In a Python environment, you can use the python-docx library to read the file. The key is to parse the template not just as a whole, but by its distinct sections as you've described (headers, paragraphs, tables, etc.).
You'll want to create a structured representation of the template. This can be a JSON object or a Python dictionary where each key represents a section (e.g., "Introduction," "Methodology") and the value is the description or content of that section. This structure is what you'll use for comparison.
Step 2: Parse and Embed the Template
Once you have the text content for each section of your template, you can proceed with creating embeddings.
 * Document Parser: Use a document parser like python-docx to extract text from headers, paragraphs, and tables. For images, you can extract associated captions or surrounding text.
 * Chunking: Breaking the text into smaller pieces is crucial for effective embedding. A chunk_size of 100 tokens with chunk_overlap of 0 is a reasonable starting point, but you may need to adjust this based on the nature of your content. For section-based analysis, it's often better to treat each section as a single "chunk" if its content is not too long.
 * Embedding: You can use Google's embeddings or any other state-of-the-art model to convert these text chunks into numerical vectors.
Step 3: Initialize Open-Source Embeddings
While you mentioned Google's embeddings, for local development or cost-saving purposes, you can use open-source models. The sentence-transformers library is an excellent choice for this. A popular model like 'all-MiniLM-L6-v2' is lightweight and effective for semantic similarity tasks.
from sentence_transformers import SentenceTransformer
embedding_model = SentenceTransformer('all-MiniLM-L6-v2')

You would then use embedding_model.encode(chunks) to generate the embeddings for your text chunks.
Step 4: Process and Embed the Second Document
This step mirrors Step 2 but is applied to the second Word document (the one you want to evaluate).
 * Upload: Use the same file upload mechanism for the second document.
 * Parse and Chunk: Apply the same parsing and chunking strategy. It's important to be consistent in how you process both documents to ensure a fair comparison.
 * Embed and Store: Generate embeddings for the chunks of the second document and store them in a vector store like FAISS or ChromaDB. This allows for efficient similarity searching.
Step 5: Create a Prompt Template and Perform Comparison with LangChain
This is where the LLM does the heavy lifting. LangChain is a powerful framework for building LLM-powered applications.
 * Similarity Search: For each section in your template, take its embedding and perform a similarity search against the vector store of the second document. This will retrieve the most relevant chunks from the second document for that particular section.
 * Prompt Template: Create a well-defined prompt template. This guides the LLM on how to perform the comparison. The prompt should include:
   * The template section's description.
   * The content of the most relevant chunks from the second document.
   * A clear instruction to evaluate if the section is present and how well the content matches the description.
Here’s an example of what the prompt might look like:
You are a document analysis assistant. Based on the template section description and the provided content from the document being reviewed, please perform the following:
1.  Determine if this section is present in the document.
2.  Assess the relevance of the provided content to the section description on a scale of 1 to 10.
3.  Provide a brief feedback on what is missing or how it could be improved.

**Template Section:** "Project Overview - This section should provide a high-level summary of the project goals, scope, and key deliverables."

**Retrieved Content from Document 2:** "This document outlines the primary objectives for the Q3 initiative. We will focus on market expansion and product development. The main outcomes will be a new software module and a report on user engagement."

Your analysis:

 * LangChain Chain: Use a LangChain "chain" (like LLMChain or a more complex custom chain) to automate the process of feeding this prompt to the LLM for each section of your template.
Step 6: Create a Streamlit UI
Streamlit is an excellent choice for creating a simple and interactive web application for this task.
 * File Uploaders: Use st.file_uploader to allow users to upload both the template and the document to be analyzed.
 * Processing Button: A button to trigger the comparison process.
 * Displaying Feedback: Organize the feedback in a clear and readable format. You could use st.expander for each section of the template. Inside each expander, you can display:
   * The template section description.
   * A status (e.g., "Found," "Missing").
   * The relevance score.
   * The LLM's qualitative feedback.
This user-friendly interface will make the results of your complex analysis pipeline easy to understand for any user.



   Of course. Here is the end-to-end code for the Word document comparison tool, broken down into two main files: one for the core logic and one for the Streamlit application.
Prerequisites
First, you need to install all the required libraries.
1. Create a requirements.txt file:
streamlit
python-docx
langchain
langchain-community
sentence-transformers
faiss-cpu
langchain-openai

2. Install the libraries:
pip install -r requirements.txt

3. Get an OpenAI API Key:
You will need an API key from OpenAI to use their LLM for the comparison step. You can get one from the OpenAI platform.
File 1: document_comparator.py
This file will contain the backend logic for parsing documents, creating embeddings, and performing the comparison with the LLM.
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


File 2: app.py
This is the main file for the Streamlit user interface. Save it in the same directory as document_comparator.py.
import streamlit as st
from document_comparator import parse_word_document, create_vector_store, get_comparison_feedback
from langchain_community.embeddings import SentenceTransformerEmbeddings
from langchain_openai import ChatOpenAI

# --- Streamlit UI Configuration ---
st.set_page_config(page_title="Document Comparator AI", layout="wide")

st.title("📄 AI-Powered Word Document Comparator")
st.markdown("""
Welcome! This tool helps you compare two Word documents. 
1.  Upload a **Template Document** which defines the desired structure and content.
2.  Upload the **Document to Compare** against the template.
3.  The AI will analyze the second document for section presence and content relevance.

**Note:** Sections are identified by 'Heading' styles in your Word documents.
""")

# --- Sidebar for Inputs ---
with st.sidebar:
    st.header("⚙️ Configuration")
    
    # Input for OpenAI API Key
    api_key = st.text_input("Enter your OpenAI API Key", type="password")

    # File Uploader for Template Document
    template_file = st.file_uploader("1. Upload Template Word Document (.docx)", type=["docx"])

    # File Uploader for Second Document
    doc_to_compare_file = st.file_uploader("2. Upload Document to Compare (.docx)", type=["docx"])

    # Comparison Button
    compare_button = st.button("🚀 Compare Documents")

# --- Main Page for Displaying Results ---

if compare_button:
    # --- Input Validation ---
    if not api_key:
        st.error("❌ Please enter your OpenAI API Key in the sidebar.")
    elif not template_file:
        st.error("❌ Please upload the Template Word Document.")
    elif not doc_to_compare_file:
        st.error("❌ Please upload the Document to Compare.")
    else:
        with st.spinner("Analyzing documents... This may take a moment."):
            try:
                # --- Step 1 & 2: Parse Template and Initialize Embeddings ---
                template_sections = parse_word_document(template_file)
                if not template_sections:
                    st.error("Could not parse the template document. Ensure it has text and headings.")
                    st.stop()

                # Initialize open-source embedding model
                embedding_model = SentenceTransformerEmbeddings(model_name="all-MiniLM-L6-v2")

                # --- Step 3: Parse and Embed Second Document ---
                doc_to_compare_sections = parse_word_document(doc_to_compare_file)
                vector_store = create_vector_store(doc_to_compare_sections, embedding_model)
                
                # --- Step 4: Initialize LLM and Perform Comparison ---
                llm = ChatOpenAI(api_key=api_key, model="gpt-3.5-turbo", temperature=0)
                
                feedback_report = get_comparison_feedback(template_sections, vector_store, llm)

                # --- Step 5: Display Feedback Report ---
                st.success("✅ Analysis Complete!")
                st.header("Comparison Feedback Report")

                for item in feedback_report:
                    with st.expander(f"**Section: {item['section']}**", expanded=True):
                        # Use markdown to format the output nicely
                        st.markdown(item['analysis'])

            except Exception as e:
                st.error(f"An unexpected error occurred: {e}")

else:
    st.info("Please upload both documents and click 'Compare Documents' to start.")


How to Run the Application
 * Save the files: Save the first code block as document_comparator.py and the second as app.py in the same folder.
 * Create Test Documents:
   * template.docx: Create a Word document with headings like "Introduction", "Methodology", and "Conclusion", each followed by a few paragraphs describing what should be in that section. Make sure to apply "Heading 1" or "Heading 2" styles to your titles.
   * document_to_compare.docx: Create another document. You can include some of the sections from the template, omit others, and write content that is either relevant or irrelevant to test the AI's feedback.
 * Run from your terminal:
   streamlit run app.py

Your web browser will open with the Streamlit application running. You can then enter your API key, upload your documents, and see the AI-generated feedback.

            streamlit run app.py






-----********************

Of course. Here is a Mermaid flowchart that visually represents the end-to-end code for the document comparison tool.
This flowchart outlines the process from user interaction in the Streamlit UI to the backend processing, analysis by the LLM, and final display of the results.
graph TD
    subgraph "1. User Interface (app.py)"
        A(Start) --> B{"User provides API Key, Template DOCX, and Comparison DOCX"};
        B --> C[Clicks 'Compare Documents'];
        C --> D{Inputs Valid?};
        D -- No --> E[Show Error Message];
        D -- Yes --> F[Show 'Analyzing...' Spinner];
    end

    subgraph "2. Backend Processing (document_comparator.py)"
        F --> G[Parse Template DOCX to get 'template_sections'];
        F --> H[Parse Comparison DOCX to get 'compare_sections'];
        
        H --> I[Initialize Embedding Model (all-MiniLM-L6-v2)];
        I --& H --> J[Create FAISS Vector Store from 'compare_sections'];
        
        G --& J --> K[Initialize LLM (OpenAI)];
        
        subgraph "Loop for each Template Section"
            K --> L[Start Loop];
            L --> M["Similarity Search: Find content in Vector Store relevant to the template section"];
            M --> N["Format Prompt: Combine template section, its content, and the retrieved document content"];
            N --> O["Query LLM for Analysis"];
            O --> P["Receive Feedback: Presence, Relevance Score, and Textual Analysis"];
            P --> Q[Append structured feedback to the final report];
            Q --> R{All sections processed?};
            R -- No --> M;
        end

        R -- Yes --> S[Return Final Feedback Report];
    end

    subgraph "3. Display Results (app.py)"
        S --> T[Render the complete report in the Streamlit UI using expanders];
        T --> U(End);
    end


How the Flowchart Works:
 * User Interface: The process begins in the Streamlit UI (app.py). The user provides all necessary inputs and clicks the compare button. The application validates these inputs before proceeding.
 * Backend Processing:
   * The core logic in document_comparator.py takes over.
   * Both the template and the comparison documents are parsed to extract their structure based on headings.
   * A vector store (a numerical representation) is created from the content of the document being compared. This allows for efficient searching.
   * The application then iterates through each section defined in the template document.
 * Comparison Loop:
   * For every section in the template, the system searches the vector store to find the most similar or relevant content in the second document.
   * This retrieved content, along with the original template section's description, is sent to the Large Language Model (LLM) with a specific prompt.
   * The LLM analyzes the information and provides a structured feedback, including whether the section was found, a relevance score, and qualitative notes.
   * This feedback is collected for all sections.
 * Display Results: Once the analysis is complete for all sections, the final report is passed back to the Streamlit UI, which then displays it to the user in a clean, organized format.
