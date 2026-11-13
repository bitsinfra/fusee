// Buttons: variants and sizes over the theme's tokens.
import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";
import { Loader2 } from "lucide-react";
import * as React from "react";
import { cn } from "../lib/cn";

export const buttonVariants = cva(
  "relative inline-flex shrink-0 cursor-pointer select-none items-center justify-center gap-1.5 whitespace-nowrap rounded-md border transition-interactive focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ds-focus disabled:pointer-events-none disabled:opacity-50 [&_svg]:pointer-events-none [&_svg]:shrink-0 [&_svg]:opacity-80",
  {
    variants: {
      variant: {
        default:
          "border-ds-accent bg-ds-accent font-semibold text-ds-accent-foreground shadow-xs hover:border-ds-accent-hover hover:bg-ds-accent-hover active:bg-ds-accent-active",
        outline:
          "border-ds-border bg-ds-surface text-ds-text-strong shadow-xs hover:bg-ds-hover active:bg-ds-active dark:bg-ds-surface-elevated",
        ghost: "border-transparent bg-transparent text-ds-text-muted shadow-none hover:bg-ds-hover hover:text-ds-text-strong active:bg-ds-active",
        secondary:
          "border-ds-border-subtle bg-ds-surface-elevated text-ds-text-strong hover:border-ds-border hover:bg-ds-hover active:bg-ds-active",
        link: "h-auto border-transparent bg-transparent p-0 text-ds-accent-text shadow-none underline-offset-4 hover:underline",
        destructive:
          "border-ds-error bg-ds-error font-semibold text-destructive-foreground hover:border-ds-error-hover hover:bg-ds-error-hover active:bg-ds-error-active",
        "destructive-outline": "border-ds-error-muted bg-transparent text-ds-error hover:bg-ds-error-muted",
      },
      size: {
        xs: "h-[var(--ds-button-height-compact)] px-2 text-ui-caption [&_svg]:size-3",
        sm: "h-[var(--ds-button-height-compact)] px-2 text-ui-body [&_svg]:size-3.5",
        default: "h-[var(--ds-button-height)] px-2.5 text-ui-body [&_svg]:size-3.5",
        lg: "h-[var(--ds-button-height-large)] px-3 text-ui-body [&_svg]:size-4",
        icon: "h-[var(--ds-button-height)] w-[var(--ds-button-height)] [&_svg]:size-4",
        "icon-sm": "h-[var(--ds-button-height-compact)] w-[var(--ds-button-height-compact)] [&_svg]:size-3.5",
      },
    },
    defaultVariants: { variant: "default", size: "default" },
  },
);

export interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement>, VariantProps<typeof buttonVariants> {
  asChild?: boolean;
  loading?: boolean;
}

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, size, asChild = false, loading = false, disabled, children, ...props }, ref) => {
    const Comp = asChild ? Slot : "button";
    return (
      <Comp
        ref={ref}
        className={cn(buttonVariants({ variant, size, className }))}
        disabled={asChild ? undefined : disabled || loading}
        aria-busy={loading || undefined}
        {...props}
      >
        {/* A slot takes exactly one child: the element it renders as. */}
        {asChild ? (
          children
        ) : (
          <>
            {loading ? <Loader2 className="animate-spin motion-reduce:animate-none" aria-hidden="true" /> : null}
            {children}
          </>
        )}
      </Comp>
    );
  },
);
Button.displayName = "Button";

export const IconButton = React.forwardRef<HTMLButtonElement, ButtonProps & { label: string }>(
  ({ label, variant = "ghost", size = "icon", ...props }, ref) => (
    <Button ref={ref} variant={variant} size={size} aria-label={label} title={label} {...props} />
  ),
);
IconButton.displayName = "IconButton";
