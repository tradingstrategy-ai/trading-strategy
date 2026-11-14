# Contributor guidance

## Docstrings and comments

- Write for a maintainer who knows Python but may not know the protocol. Give the context needed to understand the decision.
- Explain why the code exists and how it addresses the problem. Describe assumptions, units, ownership, timing, side effects and failure behaviour where they affect correct use. Do not merely narrate the statements below a comment.
- Use plain language. Replace jargon and unexplained abbreviations with concrete descriptions; retain exact API names and explain unfamiliar protocol terms on first use.
- Keep explanations proportionate: a concise summary and relevant rationale are more useful than padding. Keep comments accurate when behaviour changes, and link to authoritative sources for non-obvious protocol constraints.
- Use Sphinx reStructuredText for Python docstrings and `#:` for documented fields. Document parameters and return values when their meaning is not clear from the signature.
- Explain the regression a test prevents and why any mocked dependency is excluded from the test.
- Use UK English and sentence-case headings.
