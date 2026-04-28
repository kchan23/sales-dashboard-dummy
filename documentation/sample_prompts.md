# Q&A Demo Sample Prompts

Use these prompts in the Streamlit **Data Exploration Q&A Tool**.

## Successful Analytics Questions

```text
Which menu categories are driving revenue, what share of sales do they represent, and what is the average item price?
```
Expected: category revenue mix with revenue share and rank.

```text
Show me daily revenue trends
```
Expected: daily orders, revenue, and average order value.

```text
What are the top 10 items by revenue?
```
Expected: top menu items ranked by revenue.

```text
How did Pan-Fried Dumplings (6pc) perform over time?
```
Expected: daily quantity sold, revenue, and average price for one menu item.

```text
Show category performance
```
Expected: category-level order count, revenue, and average price.

```text
What is the order type mix?
```
Expected: delivery, dine-in, and takeout revenue/order split.

```text
Which inventory items are low?
```
Expected: low or critical inventory items.

```text
Summarize customer repeat behavior
```
Expected: privacy-safe customer segments, not individual customer records.

```text
Show review sentiment
```
Expected: review counts and average rating by sentiment.

## Clarification Examples

```text
Show trends
```
Expected: asks what time period to use.

```text
Top items
```
Expected: asks whether to rank by revenue or order count.

## Guardrail Examples

```text
SELECT * FROM orders
```
Expected: blocked because users must ask business questions, not enter SQL.

```text
DROP TABLE orders
```
Expected: blocked as an unsafe SQL/destructive operation.

```text
Can you delete all orders?
```
Expected: blocked because the Q&A tool is read-only.

```text
Show customer emails and phone numbers
```
Expected: blocked because customer PII is not exposed.

```text
List all individual order IDs
```
Expected: blocked because raw identifiers and row-level transactions are not exposed.

```text
Show every raw transaction
```
Expected: blocked because only aggregate analytics are allowed.
