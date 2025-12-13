import { useThemeStore } from "@/stores/themeStore";

export function useTheme() {
  const { theme, setTheme, getEffectiveTheme } = useThemeStore();

  return {
    theme,
    effectiveTheme: getEffectiveTheme(),
    setTheme,
    toggleTheme: () => {
      const current = getEffectiveTheme();
      setTheme(current === "dark" ? "light" : "dark");
    },
  };
}
