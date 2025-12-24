/**
 * Markdown Renderer Component
 *
 * Uses the `marked` library to parse markdown and render it as HTML.
 * This is more reliable than react-markdown for complex content.
 */

import { useMemo } from 'react';
import { marked } from 'marked';

interface MarkdownRendererProps {
  content: string;
  className?: string;
}

// Configure marked for GFM (GitHub Flavored Markdown)
marked.setOptions({
  gfm: true,
  breaks: true,
});

export function MarkdownRenderer({ content, className = '' }: MarkdownRendererProps) {
  const html = useMemo(() => {
    try {
      // Parse markdown to HTML
      const parsed = marked.parse(content);
      return typeof parsed === 'string' ? parsed : '';
    } catch (error) {
      console.error('Markdown parsing error:', error);
      return `<p>${content}</p>`;
    }
  }, [content]);

  return (
    <div
      className={className}
      dangerouslySetInnerHTML={{ __html: html }}
    />
  );
}

export default MarkdownRenderer;
