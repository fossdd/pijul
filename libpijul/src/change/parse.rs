use nom::branch::alt;
use nom::bytes::complete::*;
use nom::character::complete::*;
use nom::combinator::*;
use nom::error::ParseError;
use nom::multi::*;
use nom::sequence::*;
use nom::*;

use crate::change::printable::*;
use PrintableHunk::*;

use super::*;

fn parse_file_move_v_hunk(i: &str) -> IResult<&str, PrintableHunk> {
    let (i, path) = preceded(delimited(space0, tag("Moved:"), space0), parse_string)(i)?;
    let (i, name) = preceded(space0, parse_string)(i)?;
    let (i, perms) = preceded(space0, parse_perms)(i)?;
    let (i, pos) = preceded(space0, parse_printable_pos)(i)?;
    let (i, _) = tuple((space0, newline))(i)?;

    let (i, del) = parse_edges(i)?;

    let (i, up_context) = preceded(pair(space0, tag("up")), parse_context)(i)?;
    let (i, down_context) = preceded(pair(space0, tag(", down")), parse_context)(i)?;
    let (i, _) = newline(i)?;
    Ok((
        i,
        FileMoveV {
            path,
            name,
            perms,
            pos,
            up_context,
            down_context,
            del,
        },
    ))
}

fn parse_file_move_e_hunk(i: &str) -> IResult<&str, PrintableHunk> {
    let (i, path) = preceded(delimited(space0, tag("Moved:"), space0), parse_string)(i)?;
    let (i, pos) = preceded(space0, parse_printable_pos)(i)?;
    let (i, _) = tuple((space0, newline))(i)?;

    let (i, add) = parse_edges(i)?;
    let (i, del) = parse_edges(i)?;
    Ok((
        i,
        FileMoveE {
            path,
            pos,
            add,
            del,
        },
    ))
}

fn parse_file_del_hunk(i: &str) -> IResult<&str, PrintableHunk> {
    let (i, path) = preceded(
        delimited(space0, tag("File deletion:"), space0),
        parse_string,
    )(i)?;
    let (i, pos) = preceded(space0, parse_printable_pos)(i)?;
    let (i, encoding) = preceded(space0, parse_encoding)(i)?;
    let (i, _) = tuple((space0, newline))(i)?;
    let (i, del_edges) = parse_edges(i)?;
    let (i, content_edges) = if let Ok(x) = parse_edges(i) {
        x
    } else {
        (i, Vec::new())
    };
    let (i, contents) = parse_contents('-', encoding.clone(), i)?;
    Ok((
        i,
        FileDel {
            path,
            pos,
            encoding,
            del_edges,
            content_edges,
            contents,
        },
    ))
}

fn parse_file_undel_hunk(i: &str) -> IResult<&str, PrintableHunk> {
    let (i, path) = preceded(
        delimited(space0, tag("File un-deletion:"), space0),
        parse_string,
    )(i)?;
    let (i, pos) = preceded(space0, parse_printable_pos)(i)?;
    let (i, encoding) = preceded(space0, parse_encoding)(i)?;
    let (i, _) = tuple((space0, newline))(i)?;
    let (i, undel_edges) = parse_edges(i)?;
    let (i, content_edges) = if let Ok(x) = parse_edges(i) {
        x
    } else {
        (i, Vec::new())
    };
    let (i, contents) = parse_contents('+', encoding.clone(), i)?;
    Ok((
        i,
        FileUndel {
            path,
            pos,
            encoding,
            undel_edges,
            content_edges,
            contents,
        },
    ))
}

fn parse_file_addition_hunk(i: &str) -> IResult<&str, PrintableHunk> {
    let (i, name) = preceded(
        delimited(space0, tag("File addition:"), space0),
        parse_string,
    )(i)?;
    let (i, parent) = preceded(delimited(space1, tag("in"), space1), parse_string)(i)?;
    let (i, perms) = preceded(space0, parse_perms)(i)?;
    let (i, encoding) = preceded(space0, parse_encoding)(i)?;
    let (i, _) = tuple((space0, newline, multispace0))(i)?;

    let (i, up_context) = preceded(tag("up"), parse_context)(i)?;
    let (i, (start, end)) = delimited(space0, parse_start_end, pair(space0, newline))(i)?;
    let (i, contents) = if let PrintablePerms::IsDir = perms {
        (i, Vec::new())
    } else {
        parse_contents('+', encoding.clone(), i)?
    };
    Ok((
        i,
        FileAddition {
            name,
            parent,
            perms,
            encoding,
            up_context,
            start,
            end,
            contents,
        },
    ))
}

/// Parse a hunk header string
fn parse_edit_hunk(i: &str) -> IResult<&str, PrintableHunk> {
    debug!("parse_edit_hunk {:?}", i);
    let (i, path) = preceded(delimited(space0, tag("Edit in"), space0), parse_string)(i)?;
    let (i, line) = preceded(char(':'), u64)(i)?;
    let (i, pos) = preceded(space0, parse_printable_pos)(i)?;
    let (i, encoding) = preceded(space0, parse_encoding)(i)?;
    let (i, _) = tuple((space0, newline))(i)?;
    let (i, change) = parse_atom(i)?;
    let (i, contents) = if let Ok(s) = parse_contents('+', encoding.clone(), i) {
        if s.1.is_empty() {
            parse_contents('-', encoding.clone(), i)?
        } else {
            s
        }
    } else {
        parse_contents('-', encoding.clone(), i)?
    };
    Ok((
        i,
        Edit {
            path,
            line: line as usize,
            pos,
            encoding,
            change,
            contents,
        },
    ))
}

fn parse_replace_hunk(i: &str) -> IResult<&str, PrintableHunk> {
    let (i, path) = preceded(
        delimited(space0, tag("Replacement in"), space0),
        parse_string,
    )(i)?;
    let (i, line) = preceded(char(':'), u64)(i)?;
    let (i, pos) = preceded(space0, parse_printable_pos)(i)?;
    let (i, encoding) = preceded(space0, parse_encoding)(i)?;
    let (i, _) = tuple((space0, newline))(i)?;
    // TODO: allow newlines in between these lines
    let (i, change) = parse_edges(i)?;
    let (i, replacement) = parse_new_vertex(i)?;
    let (i, change_contents) = parse_contents('-', encoding.clone(), i)?;
    let (i, replacement_contents) = parse_contents('+', encoding.clone(), i)?;
    Ok((
        i,
        Replace {
            path,
            line: line as usize,
            pos,
            encoding,
            change,
            replacement,
            change_contents,
            replacement_contents,
        },
    ))
}

fn parse_solve_name_conflict(i: &str) -> IResult<&str, PrintableHunk> {
    let (i, path) = preceded(
        delimited(space0, tag("Solving a name conflict in"), space0),
        parse_string,
    )(i)?;
    let (i, pos) = preceded(space0, parse_printable_pos)(i)?;
    let (i, _) = tuple((space0, char(':'), space0))(i)?;
    let (i, names) = separated_list0(tuple((space0, char(','), space0)), parse_string)(i)?;
    let (i, _) = tuple((space0, newline))(i)?;
    let (i, edges) = parse_edges(i)?;
    Ok((
        i,
        SolveNameConflict {
            path,
            pos,
            names,
            edges,
        },
    ))
}

fn parse_unsolve_name_conflict(i: &str) -> IResult<&str, PrintableHunk> {
    let (i, path) = preceded(
        delimited(space0, tag("Un-solving a name conflict in"), space0),
        parse_string,
    )(i)?;
    let (i, pos) = preceded(space0, parse_printable_pos)(i)?;
    let (i, _) = tuple((space0, char(':'), space0))(i)?;
    let (i, names) = separated_list0(tuple((space0, char(','), space0)), parse_string)(i)?;
    let (i, _) = tuple((space0, newline))(i)?;
    let (i, edges) = parse_edges(i)?;
    Ok((
        i,
        UnsolveNameConflict {
            path,
            pos,
            names,
            edges,
        },
    ))
}

fn parse_solve_order_conflict(i: &str) -> IResult<&str, PrintableHunk> {
    let (i, path) = preceded(
        delimited(space0, tag("Solving an order conflict in"), space0),
        parse_string,
    )(i)?;
    let (i, line) = preceded(char(':'), u64)(i)?;
    let (i, pos) = preceded(space1, parse_printable_pos)(i)?;
    let (i, encoding) = preceded(space0, parse_encoding)(i)?;
    let (i, _) = tuple((space0, newline))(i)?;
    let (i, change) = parse_new_vertex(i)?;
    let (i, contents) = parse_contents('+', encoding.clone(), i)?;
    Ok((
        i,
        SolveOrderConflict {
            path,
            line: line as usize,
            pos,
            encoding,
            change,
            contents,
        },
    ))
}

fn parse_unsolve_order_conflict(i: &str) -> IResult<&str, PrintableHunk> {
    let (i, path) = preceded(
        delimited(space0, tag("Un-solving an order conflict in"), space0),
        parse_string,
    )(i)?;
    let (i, line) = preceded(char(':'), u64)(i)?;
    let (i, pos) = preceded(space1, parse_printable_pos)(i)?;
    let (i, encoding) = preceded(space0, parse_encoding)(i)?;
    let (i, _) = tuple((space0, newline))(i)?;
    let (i, change) = parse_edges(i)?;
    let (i, contents) = parse_contents('-', encoding.clone(), i)?;
    Ok((
        i,
        UnsolveOrderConflict {
            path,
            line: line as usize,
            pos,
            encoding,
            change,
            contents,
        },
    ))
}

fn parse_resurrect_zombies(i: &str) -> IResult<&str, PrintableHunk> {
    let (i, path) = preceded(
        delimited(space0, tag("Resurrecting zombie lines in"), space0),
        parse_string,
    )(i)?;
    let (i, line) = preceded(char(':'), u64)(i)?;
    let (i, pos) = preceded(space0, parse_printable_pos)(i)?;
    let (i, encoding) = preceded(space0, parse_encoding)(i)?;
    let (i, _) = tuple((space0, newline))(i)?;
    let (i, change) = parse_edges(i)?;
    let (i, contents) = parse_contents('+', encoding.clone(), i)?;
    Ok((
        i,
        ResurrectZombies {
            path,
            line: line as usize,
            pos,
            encoding,
            change,
            contents,
        },
    ))
}

fn parse_root_addition_hunk(i: &str) -> IResult<&str, PrintableHunk> {
    debug!("root add {:?}", i);
    let (i, _) = delimited(space0, tag("Root add"), space0)(i)?;
    let (i, _) = tuple((space0, newline, multispace0))(i)?;
    debug!("root add {:?}", i);
    let (i, up_context) = preceded(tag("up"), parse_context)(i)?;
    debug!("root add {:?}", i);
    let (i, (start, end)) = delimited(space0, parse_start_end, pair(space0, newline))(i)?;
    debug!("root add {:?}", i);
    assert_eq!(&up_context[..], &[PrintablePos(1, 0)]);
    assert_eq!(start, end);
    Ok((i, PrintableHunk::AddRoot { start }))
}

fn parse_root_deletion_hunk(i: &str) -> IResult<&str, PrintableHunk> {
    let (i, _) = delimited(space0, tag("Root del"), space0)(i)?;
    let (i, _) = tuple((space0, newline))(i)?;
    let (i, name) = parse_edges(i)?;
    let (i, inode) = parse_edges(i)?;

    Ok((i, PrintableHunk::DelRoot { inode, name }))
}

fn parse_content_line(leading_char: char, input: &str) -> IResult<&str, String> {
    preceded(
        char(leading_char),
        alt((
            map(
                delimited(tag(" "), take_till(|c| c == '\n'), newline),
                |s: &str| s.to_string() + "\n",
            ),
            map(
                delimited(tag("b"), take_till(|c| c == '\n'), newline),
                |s: &str| s.to_string(),
            ),
        )),
    )(input)
}

// TODO: better error handling
fn parse_contents(
    leading_char: char,
    encoding: Option<Encoding>,
    i: &str,
) -> IResult<&str, Vec<u8>> {
    let (i, res) = fold_many0(
        complete(|i| parse_content_line(leading_char, i)),
        String::new,
        |s, r| s + r.as_str(),
    )(i)?;
    let (i, backslash) = opt(complete(tag("\\\n")))(i)?;
    let has_encoding = encoding.is_some();
    if let Ok(mut vec) = encode(encoding, &res) {
        let not_empty = if backslash.is_some() && vec[vec.len() - 1] == b'\n' {
            vec.pop().is_some()
        } else {
            !vec.is_empty()
        };
        if has_encoding || not_empty {
            return Ok((i, vec));
        }
    }
    Err(nom::Err::Error(nom::error::Error::new(
        i,
        nom::error::ErrorKind::Verify,
    )))
}

fn encode(encoding: Option<Encoding>, contents: &str) -> Result<Vec<u8>, String> {
    if let Some(encoding) = encoding {
        Ok(encoding.encode(contents).to_vec())
    } else {
        data_encoding::BASE64
            .decode(contents.as_bytes())
            .map_err(|e| e.to_string())
    }
}

fn parse_encoding(input: &str) -> IResult<&str, Option<Encoding>> {
    map(parse_string, |e| {
        if e != BINARY_LABEL {
            Some(Encoding::for_label(&e))
        } else {
            None
        }
    })(input)
}

pub fn parse_numbered_hunk(input: &str) -> IResult<&str, (u64, PrintableHunk)> {
    tuple((terminated(u64, char('.')), parse_hunk))(input)
}

pub fn parse_hunk(input: &str) -> IResult<&str, PrintableHunk> {
    debug!("parse_hunk {:?}", input);
    alt((
        parse_file_move_v_hunk,
        parse_file_move_e_hunk,
        parse_file_del_hunk,
        parse_file_undel_hunk,
        parse_file_addition_hunk,
        parse_edit_hunk,
        parse_replace_hunk,
        parse_solve_name_conflict,
        parse_unsolve_name_conflict,
        parse_solve_order_conflict,
        parse_unsolve_order_conflict,
        parse_resurrect_zombies,
        parse_root_addition_hunk,
        parse_root_deletion_hunk,
    ))(input)
}

pub fn parse_hunks(input: &str) -> IResult<&str, Vec<(u64, PrintableHunk)>> {
    preceded(
        tuple((tag("# Hunks"), space0, newline, multispace0)),
        many0(complete(terminated(parse_numbered_hunk, multispace0))),
    )(input)
}

pub const BINARY_LABEL: &str = "binary";

pub fn encoding_label(encoding: &Option<Encoding>) -> &str {
    match encoding {
        Some(encoding) => encoding.label(),
        _ => BINARY_LABEL,
    }
}

/// Parse an escaped character: \n, \t, \r, etc.
fn parse_escaped_char(input: &str) -> IResult<&str, char> {
    preceded(
        char('\\'),
        alt((
            value('\n', char('n')),
            value('\r', char('r')),
            value('\t', char('t')),
            value('\u{08}', char('b')),
            value('\u{0C}', char('f')),
            value('\\', char('\\')),
            value('"', char('"')),
        )),
    )(input)
}

/// Parse a non-empty block of text that doesn't include \ or "
fn parse_literal<'a, E: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, &'a str, E> {
    let not_quote_slash = is_not("\"\\");
    verify(not_quote_slash, |s: &str| !s.is_empty())(input)
}

/// Combine parse_literal and parse_escaped_char into a StringFragment.
fn parse_fragment(input: &str) -> IResult<&str, StringFragment> {
    alt((
        map(parse_literal, StringFragment::Literal),
        map(parse_escaped_char, StringFragment::EscapedChar),
    ))(input)
}

/// Parse a string. Use a loop of parse_fragment and push all of the fragments
/// into an output string.
pub fn parse_string(input: &str) -> IResult<&str, String> {
    let build_string = fold_many0(parse_fragment, String::new, |mut string, fragment| {
        match fragment {
            StringFragment::Literal(s) => string.push_str(s),
            StringFragment::EscapedChar(c) => string.push(c),
        }
        string
    });
    delimited(char('"'), build_string, char('"'))(input)
}

fn parse_perms(input: &str) -> IResult<&str, PrintablePerms> {
    alt((
        value(PrintablePerms::IsDir, tag("+dx")),
        value(PrintablePerms::IsExecutable, tag("+x")),
        value(PrintablePerms::IsFile, tag("")),
    ))(input)
}

fn parse_printable_pos(input: &str) -> IResult<&str, PrintablePos> {
    map(separated_pair(u64, char('.'), u64), |(a, b)| {
        PrintablePos(a as usize, b)
    })(input)
}

fn parse_context(input: &str) -> IResult<&str, Vec<PrintablePos>> {
    delimited(space0, separated_list0(space1, parse_printable_pos), space0)(input)
}

fn parse_start_end(input: &str) -> IResult<&str, (u64, u64)> {
    preceded(
        pair(tag(", new"), space1),
        separated_pair(u64, char(':'), u64),
    )(input)
}

fn parse_edge_flags(i: &str) -> IResult<&str, PrintableEdgeFlags> {
    let (i, block) = map(opt(char('B')), |x| x.is_some())(i)?;
    let (i, folder) = map(opt(char('F')), |x| x.is_some())(i)?;
    let (i, deleted) = map(opt(char('D')), |x| x.is_some())(i)?;
    Ok((
        i,
        PrintableEdgeFlags {
            block,
            folder,
            deleted,
        },
    ))
}

fn parse_new_vertex(i: &str) -> IResult<&str, PrintableNewVertex> {
    map(
        tuple((
            space0,
            preceded(tag("up"), parse_context),
            terminated(tag(", new"), space0),
            terminated(u64, char(':')),
            terminated(u64, space0),
            preceded(tag(", down"), parse_context),
            newline,
        )),
        |(_, up_context, _, start, end, down_context, _)| PrintableNewVertex {
            up_context,
            start,
            end,
            down_context,
        },
    )(i)
}

fn parse_edge(i: &str) -> IResult<&str, PrintableEdge> {
    map(
        tuple((
            terminated(parse_edge_flags, char(':')),
            terminated(parse_edge_flags, char(' ')),
            terminated(parse_printable_pos, tag(" -> ")),
            terminated(parse_printable_pos, tag(":")),
            terminated(u64, tag("/")),
            terminated(u64, space0),
        )),
        |(previous, flag, from, to_start, to_end, introduced_by)| PrintableEdge {
            previous,
            flag,
            from,
            to_start,
            to_end,
            introduced_by: introduced_by as usize,
        },
    )(i)
}

fn parse_atom(i: &str) -> IResult<&str, PrintableAtom> {
    alt((
        map(parse_new_vertex, PrintableAtom::NewVertex),
        map(parse_edges, PrintableAtom::Edges),
    ))(i)
}

fn parse_edges(input: &str) -> IResult<&str, Vec<PrintableEdge>> {
    terminated(
        separated_list0(delimited(space0, char(','), space0), complete(parse_edge)),
        pair(space0, newline),
    )(input)
}

pub fn parse_header(input: &str) -> IResult<&str, Result<ChangeHeader, toml::de::Error>> {
    map(
        alt((take_until("# Dependencies"), take_until("# Hunks"))),
        |s| toml::de::from_str(s),
    )(input)
}

pub fn parse_dependency(i: &str) -> IResult<&str, PrintableDep> {
    let (i, mut type_) = delimited(
        char('['),
        alt((
            map(u64, |n| DepType::Numbered(n as usize, false)),
            value(DepType::ExtraKnown, char('*')),
            value(DepType::ExtraUnknown, take_till(|c| c != ']')),
        )),
        char(']'),
    )(i)?;

    let (i, plus) = terminated(
        alt((value(true, char('+')), value(false, char(' ')))),
        space0,
    )(i)?;

    // TODO: get rid of this confusing mutation
    type_ = if let DepType::Numbered(n, _) = type_ {
        DepType::Numbered(n, plus)
    } else {
        type_
    };

    let (i, hash) = delimited(
        space0,
        take_while(|c: char| c.is_ascii_alphanumeric()),
        pair(space0, char('\n')),
    )(i)?;
    Ok((
        i,
        PrintableDep {
            type_,
            hash: hash.to_string(),
        },
    ))
}

pub fn parse_dependencies(input: &str) -> IResult<&str, Vec<PrintableDep>> {
    alt((
        preceded(
            tuple((tag("# Dependencies"), space0, char('\n'), multispace0)),
            many0(terminated(parse_dependency, multispace0)),
        ),
        value(Vec::new(), multispace0),
    ))(input)
}
