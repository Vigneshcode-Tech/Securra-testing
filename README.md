#  Platform API - README

## Introduction

This application is built to **retrieve and update data from the Platform Database**. The API supports a range of modules as listed below:

- **Value-Set, Roles, and Rules**
- **Risk Score**
- **Permission and Roles Update**
- **Patient Details**
- **Questionnaire**

---

##   Getting Started

###  Installation Process

1. Clone the repository from Azure DevOps (Dev branch)  
   [https://dev.azure.com](https://dev.azure.com)  # Need to change

2. Open the project using your preferred Python IDE:  
   - Visual Studio Code  
   - PyCharm Community Edition

3. Locate the `requirements.txt` file and create a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # For Linux/Mac
venv\Scripts\activate     # For Windows
```

4. Install dependencies:

```bash
pip install -r requirements.txt
```

5. Run the application:

```bash
uvicorn _core_route:app --reload
```

---

##  Software Dependencies

Ensure the following Python libraries are installed:

- `fastapi`
- `pandas`
- `pymongo`
- `uvicorn`
- `googletrans`
- `deep_translator`
- `jinja2`
- `python-multipart`
- `openpyxl`

---

##  API Documentation

### 1. Value-Set, Roles, and Rules

#### 1.1 Value-Set

- **Endpoint**: `/execute_action`
- **Method**: `POST`
- **Action**: `truncate_insert` or `update`
- **Description**: Truncate or update the value-set collection using `OrgCode` and `ENV`.
- **Request Body**: Excel data and `OrgCode`

#### 1.2 Roles

- **Endpoint**: `/execute_action`
- **Method**: `POST`
- **Action**: `roles`
- **Description**: Updates 12 existing documents and inserts 3 new ones in `roles`. Updates `modulePermission` in `permissions`.
- **Request Body**: `OrgCode`

#### 1.3 Rules

- **Endpoint**: `/execute_action`
- **Method**: `POST`
- **Action**: `rules`
- **Description**: Adds new unique rules using `rule.code`. Requires Excel data.
- **Request Body**: Excel data and `OrgCode`

---

### 2. Risk Score

- **Endpoint**: `/documents`
- **Method**: `POST`
- **Description**: Retrieves records from the `risk-score-logs` collection.
- **Request Body**: `Env`, `AsserterId`, and optionally `start_Date`, `end_Date`

---

### 3. Permission and Roles Update

- **Endpoints**: `/updatePermission` or `/updateRoles`
- **Method**: `POST`
- **Description**: Updates `modelPermission` in `permissions` or `roles`, excluding SuperAdmin.
- **Request Body**: `Env`, and target `collectionName`

---

### 4. Patient Details

- **Endpoints**: `/submit` and `/download-careplans`
- **Method**: `POST`
- **Description**: Retrieves user details using mobile number and allows Excel export.
- **Request Body**: `Env` and Patient's Mobile Number

---

### 5. Questionnaire

- **Endpoint**: `/Insert_question`
- **Method**: `POST`
- **Description**: Creates questions and questionnaire entries based on organization.
- **Request Body**: `Env`, `QuestionnaireIds`, `Org_details`, `Fac_Details`, `roles_details`

---

