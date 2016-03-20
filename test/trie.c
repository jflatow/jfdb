#include <stdio.h>
#include <stdlib.h>
#include "../src/repl.h"

int main(int argc, char **argv) {
  size_t O = argc > 1 ? atoi(argv[1]) : 0;
  size_t B = argc > 2 ? atoi(argv[2]) : 1;
  JFT_Buffer
    *scratch = JFT_membuf(),
    *outputW = JFT_membuf(),
    *outputX = JFT_membuf(),
    *outputY = JFT_membuf();
  if (scratch == NULL || outputX == NULL || outputY == NULL)
    return ENoMem;

  JFT_Word *b = malloc(B * sizeof(JFT_Word));
  JFT_Leaf v = ((JFT_Leaf) {.size = B, .data = b});
  if (B > 0)
    b[0] = 42;
  const JFT_Stem k[] = {
    JFTK("hello world"),
    JFTK("i1"),
    JFTK("i2"),
    JFTK("index3"),
    JFTK("index3"),
    JFTK("aliens"),
    JFTK("attack!!"),
    JFTK("keyhole")
  };
  JFT *w = JFT_atom(k + 0, &v, k + 1, 7, scratch, outputW);
  JFT *x = JFT_atom(k + 1, &v, k + 1, 7, scratch, outputX);
  JFT *y = JFT_atom(k + 2, &v, k + 2, 6, scratch, outputY);

  JFT_Cursor cursors[] = {
    JFT_cursor(w),
    JFT_cursor(x),
    JFT_cursor(y)
  };
  JFT *z = JFT_cursor_merge_new(cursors, 3, scratch, 0);
  if (z) {
    printf("scratch: %zu\n", scratch->size);
    print_node(z + O);
    print_trie(z + O);

    JFT_Cursor c = JFT_cursor(z + O);
    JFT_Stem q = (JFT_Stem) {.data = malloc(JFT_KEY_LIMIT)};
    JFT_Keys ks = JFT_keys(&c, &q, Reverse);
    uint8_t keyData[JFT_KEY_LIMIT];
    print_stem(JFT_key(c.node, keyData));
    while (JFT_keys_next(&ks)) {
      printf(" -> ");
      print_stem(*ks.stem);
    }
    free(q.data);
  } else {
    printf("Failed to splice atom\n");
  }

  free(b);
  JFT_membuf_free(scratch);
  JFT_membuf_free(outputW);
  JFT_membuf_free(outputX);
  JFT_membuf_free(outputY);
  return 0;
}
