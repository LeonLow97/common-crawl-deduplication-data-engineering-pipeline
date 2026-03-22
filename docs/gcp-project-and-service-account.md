[⬅️ Back to Main Menu](../README.md)

# Create GCP Project

<img src="./diagrams/gcp-project.png" />

# Create Service Account

- IAM and Admin > Service Accounts > Create Service Account

<img src="./diagrams/gcp-service-account-name.png" />

---

- For permissions, we will grant the following roles to the service account:
  - Storage Admin (for managing Cloud Storage buckets and objects)
  - BigQuery Admin (for managing BigQuery datasets and tables)

<img src="./diagrams/gcp-service-account-permissions.png" />

---

## Download Service Account Key

- IAM and Admin > Service Accounts > [Select Service Account] > Keys > Add Key > Create New Key > JSON

### View Service Accounts

<img src="./diagrams/gcp-list-of-service-accounts.png" />

### Create Key

<img src="./diagrams/gcp-service-account-add-key.png" />

- Select **JSON**.

<img src="./diagrams/gcp-service-account-key.png" />

---

<img src="./diagrams/gcp-service-account-key-active.png" />
