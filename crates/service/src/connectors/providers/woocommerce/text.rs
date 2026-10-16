//! WooCommerce stores titles and descriptions as HTML. The catalog wants plain
//! text.

/// Plain text of an HTML fragment: tags dropped, block ends and `<br>` become
/// line breaks, entities decoded, whitespace collapsed.
pub fn plain_text(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut chars = html.char_indices().peekable();
    while let Some((index, ch)) = chars.next() {
        if ch != '<' {
            out.push(ch);
            continue;
        }
        let rest = &html[index..];
        let starts_tag = rest[1..]
            .chars()
            .next()
            .is_some_and(|next| next.is_ascii_alphabetic() || next == '/' || next == '!');
        let Some(end) = rest.find('>').filter(|_| starts_tag) else {
            out.push(ch);
            continue;
        };
        let tag = rest[1..end].trim().trim_start_matches('/');
        let name: String = tag
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric())
            .collect::<String>()
            .to_ascii_lowercase();
        if matches!(
            name.as_str(),
            "br" | "p"
                | "div"
                | "li"
                | "ul"
                | "ol"
                | "tr"
                | "h1"
                | "h2"
                | "h3"
                | "h4"
                | "h5"
                | "h6"
        ) {
            out.push('\n');
        }
        let skip_to = index + end;
        while let Some(&(next, _)) = chars.peek() {
            if next <= skip_to {
                chars.next();
            } else {
                break;
            }
        }
    }
    collapse(&decode_entities(&out))
}

/// A single-line title: entities decoded, tags dropped, whitespace collapsed.
pub fn title(html: &str) -> String {
    plain_text(html).replace('\n', " ").trim().to_string()
}

fn collapse(text: &str) -> String {
    let mut lines: Vec<String> = Vec::new();
    for line in text.lines() {
        let squeezed = line.split_whitespace().collect::<Vec<_>>().join(" ");
        if squeezed.is_empty() {
            if lines.last().is_some_and(|last| !last.is_empty()) {
                lines.push(String::new());
            }
        } else {
            lines.push(squeezed);
        }
    }
    while lines.last().is_some_and(|last| last.is_empty()) {
        lines.pop();
    }
    lines.join("\n")
}

fn decode_entities(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find('&') {
        out.push_str(&rest[..start]);
        let after = &rest[start + 1..];
        match after.find(';').filter(|end| *end <= 10) {
            Some(end) => match entity(&after[..end]) {
                Some(decoded) => {
                    out.push(decoded);
                    rest = &after[end + 1..];
                }
                None => {
                    out.push('&');
                    rest = after;
                }
            },
            None => {
                out.push('&');
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

fn entity(name: &str) -> Option<char> {
    if let Some(number) = name.strip_prefix('#') {
        let code = match number.strip_prefix(['x', 'X']) {
            Some(hex) => u32::from_str_radix(hex, 16).ok()?,
            None => number.parse().ok()?,
        };
        return char::from_u32(code);
    }
    Some(match name {
        "amp" => '&',
        "lt" => '<',
        "gt" => '>',
        "quot" => '"',
        "apos" => '\'',
        "nbsp" => ' ',
        "ndash" => '\u{2013}',
        "mdash" => '\u{2014}',
        "hellip" => '\u{2026}',
        "lsquo" => '\u{2018}',
        "rsquo" => '\u{2019}',
        "ldquo" => '\u{201C}',
        "rdquo" => '\u{201D}',
        "laquo" => '\u{AB}',
        "raquo" => '\u{BB}',
        "euro" => '\u{20AC}',
        "pound" => '\u{A3}',
        "copy" => '\u{A9}',
        "reg" => '\u{AE}',
        "auml" => '\u{E4}',
        "ouml" => '\u{F6}',
        "uuml" => '\u{FC}',
        "Auml" => '\u{C4}',
        "Ouml" => '\u{D6}',
        "Uuml" => '\u{DC}',
        "szlig" => '\u{DF}',
        "eacute" => '\u{E9}',
        "egrave" => '\u{E8}',
        "agrave" => '\u{E0}',
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_markup_and_keeps_paragraph_breaks() {
        assert_eq!(
            plain_text("<p>First line.</p>\n<p>Second &amp; <strong>last</strong>.</p>"),
            "First line.\n\nSecond & last."
        );
        assert_eq!(plain_text("a<br />b<br/>c"), "a\nb\nc");
    }

    #[test]
    fn decodes_numeric_and_named_entities() {
        assert_eq!(
            title("Hoodie &#8211; Blau &amp; Gr&uuml;n"),
            "Hoodie \u{2013} Blau & Gr\u{FC}n"
        );
        assert_eq!(title("&#x41;&#66;"), "AB");
        assert_eq!(title("5 &lt; 6 &unknown; &"), "5 < 6 &unknown; &");
    }

    #[test]
    fn leaves_plain_text_and_stray_angle_brackets_alone() {
        assert_eq!(plain_text("2 < 3 and 4 > 1"), "2 < 3 and 4 > 1");
        assert_eq!(plain_text(""), "");
    }
}
