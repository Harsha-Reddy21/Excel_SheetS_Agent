"""
LangChain-based Google Sheets Agent
"""
import os
from typing import Dict, Any
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.prompts import ChatPromptTemplate
from langchain.memory import ConversationBufferMemory
from langchain.schema import SystemMessage, HumanMessage
from agents.langchain_tools import SHEETS_TOOLS, set_current_sheet_for_tools, sheets_service

class LangChainSheetsAgent:
    """
    Advanced Google Sheets Agent powered by LangChain
    """
    
    def __init__(self):
        self.llm = self._initialize_llm()
        self.memory = self._initialize_memory()
        self.tools = SHEETS_TOOLS
        self.agent_executor = self._create_agent_executor()
        self.current_sheet = None
    
    def _initialize_llm(self) -> ChatGoogleGenerativeAI:
        """Initialize the Gemini LLM"""
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY not found in environment variables")
        
        return ChatGoogleGenerativeAI(
            model="gemini-2.0-flash",
            google_api_key=api_key,
            temperature=0.1,  
            convert_system_message_to_human=True
        )
    
    def _initialize_memory(self) -> ConversationBufferMemory:
        """Initialize conversation memory"""
        return ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=True,
            output_key="output"
        )
    
    def _create_agent_executor(self) -> AgentExecutor:
        """Create the LangChain agent executor"""
        
        # Create system prompt without sheet_name variable
        system_prompt = """You are an expert Google Sheets data analyst and automation specialist.

Your capabilities include:
1. **Data Filtering**: Filter data based on any conditions using pandas query syntax
2. **Data Aggregation**: Group and summarize data using various aggregation methods
3. **Pivot Tables**: Create complex pivot tables with multiple dimensions
4. **Data Sorting**: Sort data by any column in ascending or descending order
5. **Column Management**: Add new columns with formulas or default values
6. **Row Management**: Add new rows with specific data
7. **Custom Results**: Write analysis results to specific sheet locations
8. **Worksheet Merging**: Combine multiple worksheets vertically (stack) or horizontally (join)
9. **Data Validation**: Validate data quality and automatically fix common issues
10. **Formula Evaluation**: Evaluate complex mathematical and logical formulas
11. **Chart Generation**: Create chart data with statistical insights for visualization

**Guidelines:**
- Always get sheet information first to understand the data structure
- Use exact column names from the sheet
- For filtering, use pandas query syntax (e.g., 'salary > 50000', 'name == "John"')
- When creating new sheets, use descriptive names
- Provide clear feedback about what operations were performed
- If unsure about column names or data structure, use the get_sheet_info tool first

**Available Tools:** filter, aggregate, pivot, sort, add_column, add_row, get_info, write_results, merge_worksheets, data_validation, formula_evaluation, chart_generation
"""

        # Create prompt template
        prompt = ChatPromptTemplate.from_messages([
            ("system", system_prompt),
            ("placeholder", "{chat_history}"),
            ("human", "{input}"),
            ("placeholder", "{agent_scratchpad}")
        ])
        
        # Create agent
        agent = create_tool_calling_agent(
            llm=self.llm,
            tools=self.tools, 
            prompt=prompt
        )
        
        # Create agent executor
        return AgentExecutor(
            agent=agent,
            tools=self.tools,
            memory=self.memory,
            verbose=True,  # Enable for debugging
            handle_parsing_errors=True,
            max_iterations=5,
            early_stopping_method="generate"
        )
    
    def set_current_sheet(self, sheet_name: str):
        """Set the current sheet for the session"""
        self.current_sheet = sheet_name
        set_current_sheet_for_tools(sheet_name)
        
        # Update the agent's prompt with current sheet info
        try:
            df = sheets_service.read_sheet(sheet_name)
            if not df.empty:
                columns = list(df.columns)
                sample_data = df.head(2).to_dict('records')
                
                context_msg = f"""
Current Sheet Context Updated:
- Sheet: {sheet_name}
- Columns: {columns}
- Sample Data: {sample_data}
- Total Rows: {len(df)}
"""
                self.memory.chat_memory.add_message(SystemMessage(content=context_msg))
        except Exception as e:
            print(f"Warning: Could not load sheet context: {e}")
    
    def process_query(self, user_query: str, sheet_name: str = None) -> Dict[str, Any]:
        """
        Process a user query using the LangChain agent
        
        Args:
            user_query: Natural language query from user
            sheet_name: Name of the sheet to operate on
            
        Returns:
            Dictionary with response and metadata
        """
        try:
            # Set current sheet if provided
            if sheet_name:
                self.set_current_sheet(sheet_name)
            elif not self.current_sheet:
                self.current_sheet = "Sheet1"  # Default
                self.set_current_sheet(self.current_sheet)
            
            # Enhance query with context
            enhanced_query = f"""
Sheet: {self.current_sheet}
User Request: {user_query}

Please analyze this request and use the appropriate tools to fulfill it. 
If you need to understand the data structure first, use get_sheet_info.
"""
            
            # Execute the agent
            response = self.agent_executor.invoke({
                "input": enhanced_query
            })
            
            return {
                "success": True,
                "response": response.get("output", ""),
                "sheet_name": self.current_sheet,
                "intermediate_steps": response.get("intermediate_steps", [])
            }
            
        except Exception as e:
            error_msg = f"Error processing query: {str(e)}"
            print(f"Agent Error: {error_msg}")
            
            return {
                "success": False,
                "response": error_msg,
                "sheet_name": self.current_sheet,
                "error": str(e)
            }
    
    def get_conversation_history(self) -> list:
        """Get the conversation history"""
        return self.memory.chat_memory.messages
    
    def clear_memory(self):
        """Clear the conversation memory"""
        self.memory.clear()
    
    def get_available_tools(self) -> list:
        """Get list of available tool names and descriptions"""
        return [
            {
                "name": tool.name,
                "description": tool.description,
                "args_schema": tool.args_schema.schema() if tool.args_schema else None
            }
            for tool in self.tools
        ]

# Create global instance
langchain_sheets_agent = LangChainSheetsAgent()