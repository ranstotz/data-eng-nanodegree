# Normal Forms

## First Normal Form

- Cells have single value.
- To satisfy 1NF, the values in each column of a table must be atomic. In the initial table, Subject contains a set of subject values, meaning it does not comply.

## Second Normal Form

- To conform to 2NF and remove duplicities, every non-key attribute must depend on the whole key, not just part of it.

## Third Normal Form

- Transitive dependency.
- Transitive dependency occurred because a non-key attribute (Author) was determining another non-key attribute (Author Nationality).
