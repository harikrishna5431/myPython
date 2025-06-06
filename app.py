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

