# security.py
def encode_canary(user_identifier: int) -> str:
    """Converts a user ID into an invisible watermark string."""
    binary_str = format(int(user_identifier), 'b')
    # \u200B is a zero-width space (0), \u200C is a zero-width non-joiner (1)
    canary = "".join('\u200C' if bit == '1' else '\u200B' for bit in binary_str)
    return canary

def decode_canary(leaked_text: str) -> int:
    """Extracts the user ID from leaked text."""
    binary_str = ""
    for char in leaked_text:
        if char == '\u200C': binary_str += '1'
        elif char == '\u200B': binary_str += '0'
    return int(binary_str, 2) if binary_str else None
